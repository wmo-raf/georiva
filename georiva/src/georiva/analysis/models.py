from django.db import models


class Node(models.Model):
    """
    A single node in an analysis graph.
    
    Can be:
    - Source: Load data from a Dataset
    - Transform: Apply operation to inputs
    - Temporal: Group/aggregate over time
    - Spatial: Resample/aggregate over space
    - Filter: Apply mask or filter conditions
    - Output: Save results
    """
    analysis = models.ForeignKey('analysis.Analysis', on_delete=models.CASCADE, related_name='nodes')
    name = models.CharField(max_length=100)
    
    class NodeType(models.TextChoices):
        SOURCE = 'source', 'Data Source'
        TRANSFORM = 'transform', 'Transform'
        TEMPORAL = 'temporal', 'Temporal Operation'
        SPATIAL = 'spatial', 'Spatial Operation'
        FILTER = 'filter', 'Filter/Mask'
        OUTPUT = 'output', 'Output'
    
    node_type = models.CharField(max_length=20, choices=NodeType.choices)
    
    # Configuration (interpreted based on node_type)
    config = models.JSONField(default=dict)
    
    # For transform nodes: which operator to apply
    operator = models.ForeignKey('Operator', null=True, blank=True, on_delete=models.PROTECT)
    
    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['analysis', 'name'], name='unique_node_name_per_analysis')
        ]


class Edge(models.Model):
    """
    Connection between nodes.
    """
    analysis = models.ForeignKey('Analysis', on_delete=models.CASCADE, related_name='edges')
    
    source_node = models.ForeignKey(Node, on_delete=models.CASCADE, related_name='outgoing_edges')
    target_node = models.ForeignKey(Node, on_delete=models.CASCADE, related_name='incoming_edges')
    
    # Which input slot on the target node
    input_name = models.CharField(max_length=100, default='data')
    
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['target_node', 'input_name'],
                name='unique_input_per_node'
            )
        ]


class Analysis(models.Model):
    """
    A complete analysis definition (DAG of nodes).
    """
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    
    # Who owns this
    created_by = models.ForeignKey('auth.User', on_delete=models.SET_NULL, null=True)
    is_public = models.BooleanField(default=False)
    
    # For derived datasets: auto-run when inputs update
    is_live = models.BooleanField(default=False)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def to_dict(self) -> dict:
        """Export as portable dict."""
        return {
            'name': self.name,
            'nodes': [
                {
                    'name': n.name,
                    'type': n.node_type,
                    'config': n.config,
                    'operator': n.operator.name if n.operator else None,
                }
                for n in self.nodes.all()
            ],
            'edges': [
                {
                    'source': e.source_node.name,
                    'target': e.target_node.name,
                    'input': e.input_name,
                }
                for e in self.edges.all()
            ],
        }
    
    @classmethod
    def from_dict(cls, data: dict, user=None) -> 'Analysis':
        """Create from portable dict."""
        # ... implementation
        pass
