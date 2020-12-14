import pandas as pd
import numpy as np
import geopandas as gpd


__title__ = 'Layer Fuse'
__version__ = '1.0.0'
__author__ = 'Gopindra Sivakumar Nair'
__license__ = 'MIT'
__copyright__ = '2020 by Gopindra Sivakumar Nair'


def layerfuse(into_layer, from_layer, size_cols=[], density_cols=[], show_overlap=False):
    
    """ Merges attributes of GeoDataFrame based on overlap between polygons.
    
    Parameters
    ----------
    into_layer : gpd.GeoDataFrame
        The GeoDataFrame object into which the attributes will be added.
    from_layer : gpd.GeoDataFrame
        The GeoDataFrame object based on which the attributes will be computed 
        for `into_layer`.
    size_cols : List of column indices, optional
        The column indices of attributes that are counts of features inside the 
        polygons or attributes whose values are proportional to the area of the 
        polygon.
    density_cols: List of column indices, optional
        The column indices of attributes that represent the density of features
        inside the polygons. 
    show_overlap: bool, optional
        Adds a new column named "_OVERLAP" to the returned GeoDataFrame. This 
        column shows the proportion of area of polygons in into_layer that are
        covered by the from_layer. This should always lie between 0 and 1. If
        the value is less than 1, the computed attribute is likely to be an
        underestimate since the contributions from the non-overlapping portions
        are not added. A possible fix would be to later, divide the merged
        attribute columns with the values in "_OVERLAP" column.
    
    Returns
    -------
    GeoDataFrame
        GeoDataFrame with `size_cols` and `density_cols` added.
    
    """
    
    from_layer = from_layer.filter(size_cols + density_cols + [from_layer.geometry.name])
    sjoin = gpd.sjoin(gpd.GeoDataFrame(geometry=into_layer.geometry), from_layer)
    sjoin = sjoin.reset_index()
    
    into_fraction = pd.Series(np.zeros(sjoin.shape[0]), index=sjoin.index)
    from_fraction = pd.Series(np.zeros(sjoin.shape[0]), index=sjoin.index)
    
    for index in sjoin.index:
        into_poly = sjoin.geometry[index]
        from_poly = from_layer.geometry[sjoin.loc[index, "index_right"]]
        intersection = into_poly.intersection(from_poly)
        into_fraction[index] = intersection.area/into_poly.area
        from_fraction[index] = intersection.area/from_poly.area
        
    sjoin[density_cols] = sjoin[density_cols].multiply(into_fraction, axis=0)
    sjoin[size_cols] = sjoin[size_cols].multiply(from_fraction, axis=0)
    
    if show_overlap:
        sjoin["_OVERLAP"] = into_fraction
    
    index_name = "index" if into_layer.index.name is None else into_layer.index.name
    fused_layer = sjoin.groupby(index_name).agg(sum)
    fused_layer = fused_layer.drop(["index_right"], axis=1)
    fused_layer = gpd.GeoDataFrame(into_layer.merge(fused_layer, how="left", left_index=True, right_index=True), crs=into_layer.crs)

    return(fused_layer)

def test_layerfuse():
    """ Tests the layerfuse function """
    
    print("Testing layerfuse.")
    
    # Generating a GeoDataFrame A
    from shapely.geometry import Polygon
    polygonsA = list()
    polygonsA.append(Polygon(zip([ 0,  2,  2,  0,  0],
                                 [ 0,  0,  7,  7,  0])))
    polygonsA.append(Polygon(zip([ 2,  7,  7,  2,  2],
                                 [ 3,  3,  7,  7,  3])))
    polygonsA.append(Polygon(zip([ 2,  7,  7,  2,  2],
                                 [ 0,  0,  3,  3,  0])))
    polygonsA.append(Polygon(zip([ 7, 12, 12,  7,  7],
                                 [ 2,  2,  7,  7,  2])))
    polygonsA.append(Polygon(zip([ 7, 12, 12,  7,  7],
                                 [ 0,  0,  2,  2,  0])))
    
    # Size Attributes
    x_A = [ 70, 80, 30, 75, 60 ]
    y_A = [  5,  4,  2,  3,  6 ]
    
    geodatA = gpd.GeoDataFrame({"x":x_A, "y":y_A}, geometry=polygonsA)
    
    # Generating a GeoDataFrame B
    polygonsB = list()
    polygonsB.append(Polygon(zip([ 1,  5,  5,  1,  1],
                                 [ 1,  1,  6,  6,  1])))
    polygonsB.append(Polygon(zip([ 5,  8,  8,  5,  5],
                                 [ 1,  1,  6,  6,  1])))
    polygonsB.append(Polygon(zip([ 8, 13, 13,  8,  8],
                                 [ 1,  1,  6,  6,  1])))    
    
    geodatB = gpd.GeoDataFrame(geometry=polygonsB)
    
    # Plot if needed
    # ax = geodatA.plot(edgecolor="k", facecolor="gray", alpha=0.5, linewidth=3)
    # ax = geodatB.plot(ax = ax, facecolor="cyan", legend=True, edgecolor="blue", alpha=.5, linewidth=3)
    # Note that third polygon in geodatB is not completely overlapped by geodatA.
    
    fused_layer = layerfuse(geodatB, geodatA, size_cols=["x"], density_cols=["y"], show_overlap=True)
     
    print("Before correction")
    print(fused_layer)
    
    # Correcting for partial overlap
    fused_layer[["x","y"]] = fused_layer[["x","y"]].divide(fused_layer["_OVERLAP"], axis=0)
    
    print("After correction")
    print(fused_layer)

if __name__ == "__main__":
    test_layerfuse()

