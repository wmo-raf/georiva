from django.urls import include, path

urlpatterns = [
    path("timeseries/", include("georiva.analysis.timeseries.urls")),
]
