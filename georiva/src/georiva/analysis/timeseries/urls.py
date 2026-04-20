from django.urls import path

from .views import AreaTimeseriesView, PointTimeseriesView

app_name = "timeseries"

urlpatterns = [
    path("point/", PointTimeseriesView.as_view(), name="point"),
    path("area/",  AreaTimeseriesView.as_view(),  name="area"),
]
