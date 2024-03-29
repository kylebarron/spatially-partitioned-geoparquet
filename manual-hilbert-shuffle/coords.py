"""
Vendored from
https://github.com/jorisvandenbossche/python-geoarrow/blob/80b76e74e0492a8f0914ed5331155154d0776593/src/geoarrow/coords.py
under the MIT license
"""
import numpy as np

try:
    import pygeos
    from pygeos import GeometryType
except ImportError:
    pass

# # GEOS -> coords/offset arrays


def _get_arrays_point(arr):
    # only one array of coordinates
    coords = pygeos.get_coordinates(arr)

    return coords.ravel(), None


def _get_arrays_multipoint(arr):
    # explode/flatten the MultiPoints
    arr_flat, part_indices = pygeos.get_parts(arr, return_index=True)
    # the offsets into the multipoint parts
    offsets = np.insert(np.bincount(part_indices).cumsum(), 0, 0)

    # only one array of coordinates
    coords = pygeos.get_coordinates(arr)

    return coords.ravel(), offsets


def _get_arrays_linestring(arr):
    # the coords and offsets into the coordinates of the linestrings
    coords, indices = pygeos.get_coordinates(arr, return_index=True)
    offsets = np.insert(np.bincount(indices).cumsum(), 0, 0)

    return coords.ravel(), offsets


def _get_arrays_multilinestring(arr):
    # explode/flatten the MultiLineStrings
    arr_flat, part_indices = pygeos.get_parts(arr, return_index=True)
    # the offsets into the multilinestring parts
    offsets2 = np.insert(np.bincount(part_indices).cumsum(), 0, 0)

    # the coords and offsets into the coordinates of the linestrings
    coords, indices = pygeos.get_coordinates(arr_flat, return_index=True)
    offsets1 = np.insert(np.bincount(indices).cumsum(), 0, 0)

    return coords.ravel(), (offsets1, offsets2)


def _get_arrays_polygon(arr):
    # explode/flatten the Polygons into Rings
    arr_flat2, ring_indices = pygeos.geometry.get_rings(arr, return_index=True)
    # the offsets into the exterior/interior rings of the multipolygon parts
    offsets2 = np.insert(np.bincount(ring_indices).cumsum(), 0, 0)

    # the coords and offsets into the coordinates of the rings
    coords, indices = pygeos.get_coordinates(arr_flat2, return_index=True)
    offsets1 = np.insert(np.bincount(indices).cumsum(), 0, 0)

    return coords.ravel(), (offsets1, offsets2)


def _get_arrays_multipolygon(arr):
    # explode/flatten the MultiPolygons
    arr_flat, part_indices = pygeos.get_parts(arr, return_index=True)
    # the offsets into the multipolygon parts
    offsets3 = np.insert(np.bincount(part_indices).cumsum(), 0, 0)

    # explode/flatten the Polygons into Rings
    arr_flat2, ring_indices = pygeos.geometry.get_rings(arr_flat, return_index=True)
    # the offsets into the exterior/interior rings of the multipolygon parts
    offsets2 = np.insert(np.bincount(ring_indices).cumsum(), 0, 0)

    # the coords and offsets into the coordinates of the rings
    coords, indices = pygeos.get_coordinates(arr_flat2, return_index=True)
    offsets1 = np.insert(np.bincount(indices).cumsum(), 0, 0)

    return coords.ravel(), (offsets1, offsets2, offsets3)


def get_flat_coords_offset_arrays(arr):
    geom_types = np.unique(pygeos.get_type_id(arr))
    # ignore missing values (type of -1)
    geom_types = geom_types[geom_types >= 0]

    if len(geom_types) == 1 and geom_types[0] == GeometryType.POINT:
        typ = "point"
        coords, offsets = _get_arrays_point(arr)

    elif len(geom_types) == 1 and geom_types[0] == GeometryType.LINESTRING:
        typ = "linestring"
        coords, offsets = _get_arrays_linestring(arr)

    elif len(geom_types) == 1 and geom_types[0] == GeometryType.POLYGON:
        typ = "polygon"
        coords, offsets = _get_arrays_polygon(arr)

    elif all(t in {GeometryType.POINT, GeometryType.MULTIPOINT} for t in geom_types):
        typ = "multipoint"
        coords, offsets = _get_arrays_multipoint(arr)

    elif all(
        t in {GeometryType.LINESTRING, GeometryType.MULTILINESTRING} for t in geom_types
    ):
        typ = "multilinestring"
        coords, offsets = _get_arrays_multilinestring(arr)

    elif all(
        t in {GeometryType.POLYGON, GeometryType.MULTIPOLYGON} for t in geom_types
    ):
        typ = "multipolygon"
        coords, offsets = _get_arrays_multipolygon(arr)

    else:
        raise ValueError(
            f"Geometry type combination is not supported ({list(geom_types)})"
        )

    return typ, coords, offsets


# # coords/offset arrays -> GEOS


def _point_from_flatcoords(coords):
    result = pygeos.points(coords.reshape(-1, 2))

    return result


def _multipoint_from_flatcoords(coords, offsets):
    # recreate points
    points = pygeos.points(coords.reshape(-1, 2))

    # recreate multipoints
    multipoint_parts = np.diff(offsets)
    multipoint_indices = np.repeat(np.arange(len(multipoint_parts)), multipoint_parts)
    result = pygeos.multipoints(points, indices=multipoint_indices)

    return result


def _linestring_from_flatcoords(coords, offsets):
    # recreate linestrings
    linestring_n = np.diff(offsets)
    linestring_indices = np.repeat(np.arange(len(linestring_n)), linestring_n)
    result = pygeos.linestrings(coords.reshape(-1, 2), indices=linestring_indices)

    return result


def _multilinestrings_from_flatcoords(coords, offsets1, offsets2):
    # recreate linestrings
    linestring_n = np.diff(offsets1)
    linestring_indices = np.repeat(np.arange(len(linestring_n)), linestring_n)
    linestrings = pygeos.linestrings(coords.reshape(-1, 2), indices=linestring_indices)

    # recreate multilinestrings
    multilinestring_parts = np.diff(offsets2)
    multilinestring_indices = np.repeat(
        np.arange(len(multilinestring_parts)), multilinestring_parts
    )
    result = pygeos.multilinestrings(linestrings, indices=multilinestring_indices)

    return result


def _polygon_from_flatcoords(coords, offsets1, offsets2):
    # recreate rings
    ring_lengths = np.diff(offsets1)
    ring_indices = np.repeat(np.arange(len(ring_lengths)), ring_lengths)
    rings = pygeos.linearrings(coords.reshape(-1, 2), indices=ring_indices)

    # recreate polygons
    polygon_rings_n = np.diff(offsets2)
    polygon_indices = np.repeat(np.arange(len(polygon_rings_n)), polygon_rings_n)
    result = pygeos.polygons(rings, indices=polygon_indices)

    return result


def _multipolygons_from_flatcoords(coords, offsets1, offsets2, offsets3):
    # recreate polygons
    polygons = _polygon_from_flatcoords(coords, offsets1, offsets2)

    # recreate multipolygons
    multipolygon_parts = np.diff(offsets3)
    multipolygon_indices = np.repeat(
        np.arange(len(multipolygon_parts)), multipolygon_parts
    )
    result = pygeos.multipolygons(polygons, indices=multipolygon_indices)

    return result


def get_geometries_from_flatcoords(typ, coords, offsets):

    if typ == "point":
        return _point_from_flatcoords(coords)
    if typ == "linestring":
        return _linestring_from_flatcoords(coords, offsets)
    if typ == "polygon":
        return _polygon_from_flatcoords(coords, *offsets)
    elif typ == "multipoint":
        return _multipoint_from_flatcoords(coords, offsets)
    elif typ == "multilinestring":
        return _multilinestrings_from_flatcoords(coords, *offsets)
    elif typ == "multipolygon":
        return _multipolygons_from_flatcoords(coords, *offsets)
    else:
        raise ValueError(typ)
