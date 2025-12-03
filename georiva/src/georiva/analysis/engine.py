from dataclasses import dataclass
from typing import Dict, Any

import dask.array as da
import networkx as nx
import xarray as xr

from georiva.analysis.models import Analysis, Node


@dataclass
class ExecutionPlan:
    """
    Optimized execution plan for an analysis.
    """
    graph: nx.DiGraph
    execution_order: list  # Topologically sorted nodes
    optimizations: list  # Applied optimizations
    estimated_memory: int
    estimated_time: float


class AnalysisEngine:
    """
    Executes analysis graphs with lazy evaluation and optimization.
    """
    
    def __init__(self):
        self.operator_registry = {}
        self._load_operators()
    
    def compile(self, analysis: Analysis, parameters: dict = None) -> ExecutionPlan:
        """
        Compile an analysis into an optimized execution plan.
        
        This is where query optimization happens:
        - Predicate pushdown (filter early)
        - Projection pushdown (load only needed variables)
        - Operation fusion (combine compatible ops)
        - Chunk size optimization
        """
        # Build networkx graph
        G = nx.DiGraph()
        
        for node in analysis.nodes.all():
            G.add_node(node.name, node=node)
        
        for edge in analysis.edges.all():
            G.add_edge(edge.source_node.name, edge.target_node.name, input=edge.input_name)
        
        # Topological sort
        try:
            execution_order = list(nx.topological_sort(G))
        except nx.NetworkXUnfeasible:
            raise ValueError("Analysis graph has cycles")
        
        # Apply optimizations
        optimizations = []
        G, opts = self._optimize_predicate_pushdown(G)
        optimizations.extend(opts)
        
        G, opts = self._optimize_fusion(G)
        optimizations.extend(opts)
        
        # Estimate resources
        memory_estimate = self._estimate_memory(G, parameters)
        time_estimate = self._estimate_time(G, parameters)
        
        return ExecutionPlan(
            graph=G,
            execution_order=execution_order,
            optimizations=optimizations,
            estimated_memory=memory_estimate,
            estimated_time=time_estimate,
        )
    
    def execute(
            self,
            analysis: Analysis,
            parameters: dict = None,
            progress_callback=None
    ) -> Dict[str, Any]:
        """
        Execute an analysis.
        
        Args:
            analysis: The analysis to run
            parameters: Runtime parameters (time range, etc.)
            progress_callback: Optional callback for progress updates
        
        Returns:
            Dict mapping output node names to results
        """
        plan = self.compile(analysis, parameters)
        
        # Results cache for intermediate nodes
        results: Dict[str, Any] = {}
        
        for i, node_name in enumerate(plan.execution_order):
            node_data = plan.graph.nodes[node_name]
            node = node_data['node']
            
            if progress_callback:
                progress_callback(
                    progress=(i / len(plan.execution_order)) * 100,
                    message=f"Executing {node_name}"
                )
            
            # Gather inputs from upstream nodes
            inputs = {}
            for source, target, edge_data in plan.graph.in_edges(node_name, data=True):
                input_name = edge_data.get('input', 'data')
                inputs[input_name] = results[source]
            
            # Execute node
            result = self._execute_node(node, inputs, parameters)
            results[node_name] = result
        
        # Return only output nodes
        outputs = {}
        for node in analysis.nodes.filter(node_type=Node.NodeType.OUTPUT):
            if node.name in results:
                outputs[node.name] = results[node.name]
        
        return outputs
    
    def _execute_node(
            self,
            node: Node,
            inputs: Dict[str, Any],
            parameters: dict
    ) -> Any:
        """Execute a single node."""
        
        if node.node_type == Node.NodeType.SOURCE:
            return self._execute_source(node, parameters)
        
        elif node.node_type == Node.NodeType.TRANSFORM:
            return self._execute_transform(node, inputs)
        
        elif node.node_type == Node.NodeType.TEMPORAL:
            return self._execute_temporal(node, inputs)
        
        elif node.node_type == Node.NodeType.SPATIAL:
            return self._execute_spatial(node, inputs)
        
        elif node.node_type == Node.NodeType.FILTER:
            return self._execute_filter(node, inputs)
        
        elif node.node_type == Node.NodeType.OUTPUT:
            return self._execute_output(node, inputs, parameters)
        
        else:
            raise ValueError(f"Unknown node type: {node.node_type}")
    
    def _execute_source(self, node: Node, parameters: dict) -> xr.DataArray:
        """
        Load data from a dataset.
        
        Uses lazy loading with dask for large datasets.
        """
        config = node.config
        dataset_id = config['dataset']
        
        # Determine time range
        if config.get('time_range') == 'inherit':
            start_time = parameters.get('start_time')
            end_time = parameters.get('end_time')
        else:
            start_time = config.get('start_time')
            end_time = config.get('end_time')
        
        # Load Items lazily
        return self._load_dataset_lazy(dataset_id, start_time, end_time)
    
    def _load_dataset_lazy(
            self,
            dataset_id: str,
            start_time,
            end_time
    ) -> xr.DataArray:
        """
        Load a dataset as a lazy xarray with dask backend.
        
        Data is not actually loaded until compute() is called.
        """
        from georiva.core.models import Item
        
        items = Item.objects.filter(
            dataset_id=dataset_id,
            datetime__gte=start_time,
            datetime__lte=end_time,
            status=Item.Status.READY
        ).order_by('datetime')
        
        # Build lazy array from Item references
        # Each Item becomes one slice along time dimension
        
        def load_item(item_id):
            """Lazy loader for a single item."""
            item = Item.objects.get(id=item_id)
            asset = item.assets.filter(roles__contains=['data']).first()
            return self._load_asset_data(asset)
        
        # Create dask delayed objects
        item_ids = list(items.values_list('id', flat=True))
        datetimes = list(items.values_list('datetime', flat=True))
        
        if not item_ids:
            raise ValueError(f"No items found for {dataset_id} in time range")
        
        # Get shape from first item
        first_data = load_item(item_ids[0])
        shape = first_data.shape
        
        # Build lazy dask array
        lazy_arrays = [
            da.from_delayed(
                dask.delayed(load_item)(item_id),
                shape=shape,
                dtype=np.float32
            )
            for item_id in item_ids
        ]
        
        # Stack along time dimension
        stacked = da.stack(lazy_arrays, axis=0)
        
        # Create xarray with coordinates
        return xr.DataArray(
            stacked,
            dims=['time', 'y', 'x'],
            coords={
                'time': datetimes,
                'y': first_data.coords['y'].values,
                'x': first_data.coords['x'].values,
            },
            attrs={'dataset_id': dataset_id}
        )
    
    def _execute_temporal(self, node: Node, inputs: Dict[str, xr.DataArray]) -> xr.DataArray:
        """Execute a temporal operation."""
        config = node.config
        operation = config['operation']
        data = inputs.get('data')
        
        if operation == 'group_by':
            return self._temporal_group_by(data, config['grouping'])
        
        elif operation == 'aggregate':
            method = config['method']
            dim = config.get('dim', 'time')
            return self._temporal_aggregate(data, method, dim)
        
        elif operation == 'resample':
            freq = config['freq']
            method = config['method']
            return data.resample(time=freq).reduce(getattr(np, method))
        
        elif operation == 'rolling':
            window = config['window']
            method = config['method']
            return getattr(data.rolling(time=window), method)()
        
        elif operation == 'diff':
            # Difference from previous timestep
            return data.diff(dim='time')
        
        elif operation == 'cumsum':
            return data.cumsum(dim='time')
        
        else:
            raise ValueError(f"Unknown temporal operation: {operation}")
    
    def _temporal_group_by(self, data: xr.DataArray, grouping: dict) -> xr.DataArray:
        """
        Group data by temporal periods.
        
        Adds a 'group' coordinate that can be used for subsequent aggregation.
        """
        group_type = grouping['type']
        
        if group_type == 'season':
            # Custom season grouping
            start_month = grouping['start_month']
            start_day = grouping['start_day']
            end_month = grouping['end_month']
            end_day = grouping['end_day']
            
            # Create group labels
            def get_season_label(dt):
                # Determine which season-year this datetime belongs to
                if end_month < start_month:  # Season crosses year boundary
                    if dt.month >= start_month or dt.month <= end_month:
                        year = dt.year if dt.month >= start_month else dt.year - 1
                        return f"{year}-S"
                else:
                    if start_month <= dt.month <= end_month:
                        return f"{dt.year}-S"
                return None  # Outside season
            
            times = pd.to_datetime(data.time.values)
            groups = [get_season_label(t) for t in times]
            
            # Filter out None (outside season) and add group coordinate
            valid_mask = [g is not None for g in groups]
            data = data.isel(time=valid_mask)
            valid_groups = [g for g in groups if g is not None]
            
            data = data.assign_coords(group=('time', valid_groups))
            return data
        
        elif group_type == 'month':
            data = data.assign_coords(
                group=('time', [f"{t.year}-{t.month:02d}" for t in pd.to_datetime(data.time.values)])
            )
            return data
        
        elif group_type == 'year':
            data = data.assign_coords(
                group=('time', [str(t.year) for t in pd.to_datetime(data.time.values)])
            )
            return data
        
        elif group_type == 'dekad':
            # 10-day periods (3 per month)
            def get_dekad(dt):
                dekad = min(3, (dt.day - 1) // 10 + 1)
                return f"{dt.year}-{dt.month:02d}-D{dekad}"
            
            data = data.assign_coords(
                group=('time', [get_dekad(t) for t in pd.to_datetime(data.time.values)])
            )
            return data
        
        elif group_type == 'pentad':
            # 5-day periods (6 per month)
            def get_pentad(dt):
                pentad = min(6, (dt.day - 1) // 5 + 1)
                return f"{dt.year}-{dt.month:02d}-P{pentad}"
            
            data = data.assign_coords(
                group=('time', [get_pentad(t) for t in pd.to_datetime(data.time.values)])
            )
            return data
        
        elif group_type == 'doy':
            # Day of year (for daily climatology)
            data = data.assign_coords(
                group=('time', [t.timetuple().tm_yday for t in pd.to_datetime(data.time.values)])
            )
            return data
        
        elif group_type == 'custom':
            # User-defined periods
            periods = grouping['periods']
            # ... implementation
            pass
        
        else:
            raise ValueError(f"Unknown grouping type: {group_type}")
    
    def _temporal_aggregate(
            self,
            data: xr.DataArray,
            method: str,
            dim: str
    ) -> xr.DataArray:
        """
        Aggregate data along a dimension.
        
        If data has a 'group' coordinate, aggregates within groups.
        """
        if 'group' in data.coords:
            # Aggregate within groups
            return data.groupby('group').reduce(getattr(np, f'nan{method}'))
        else:
            # Simple aggregation along dimension
            return getattr(data, method)(dim=dim)
    
    def _execute_transform(self, node: Node, inputs: Dict[str, xr.DataArray]) -> xr.DataArray:
        """Execute a transform operation."""
        config = node.config
        
        if 'expression' in config:
            # Math expression
            import numexpr as ne
            
            # Build variable dict from inputs
            local_dict = {name: arr.values for name, arr in inputs.items()}
            result = ne.evaluate(config['expression'], local_dict=local_dict)
            
            # Preserve coordinates from first input
            first_input = list(inputs.values())[0]
            return xr.DataArray(
                result,
                dims=first_input.dims,
                coords=first_input.coords,
            )
        
        elif node.operator:
            # Registered operator
            func = self.operator_registry[node.operator.name]
            return func(**inputs, **config.get('params', {}))
        
        else:
            raise ValueError("Transform node must have expression or operator")
    
    def _execute_output(
            self,
            node: Node,
            inputs: Dict[str, xr.DataArray],
            parameters: dict
    ) -> Any:
        """
        Execute output node - materialize results.
        
        This is where lazy evaluation is triggered.
        """
        config = node.config
        data = inputs.get('data')
        
        # Force computation if using dask
        if hasattr(data.data, 'compute'):
            data = data.compute()
        
        if 'dataset' in config:
            # Save as Items to a dataset
            return self._save_to_dataset(data, config['dataset'], parameters)
        
        elif config.get('format') == 'csv':
            # Export as CSV (for zonal stats, etc.)
            return self._export_csv(data, config['path'])
        
        elif config.get('format') == 'netcdf':
            # Export as NetCDF
            return self._export_netcdf(data, config['path'])
        
        else:
            # Return raw data
            return data
    
    # Optimization methods
    
    def _optimize_predicate_pushdown(self, G: nx.DiGraph):
        """
        Push filter operations as early as possible in the graph.
        """
        optimizations = []
        # ... implementation
        return G, optimizations
    
    def _optimize_fusion(self, G: nx.DiGraph):
        """
        Fuse compatible operations (e.g., multiple element-wise transforms).
        """
        optimizations = []
        # ... implementation
        return G, optimizations
