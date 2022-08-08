"""
Vendored from
https://github.com/jorisvandenbossche/python-geoarrow/blob/80b76e74e0492a8f0914ed5331155154d0776593/src/geoarrow/extension_types.py
under the MIT license
"""
import numpy as np
import pyarrow as pa

from coords import get_flat_coords_offset_arrays, get_geometries_from_flatcoords


class ArrowGeometryArray(pa.ExtensionArray):
    @property
    def values(self):
        return self.storage.values

    @property
    def offsets(self):
        return self.storage.offsets

    def to_numpy(self, **kwargs):
        return construct_numpy_array(self.storage, self.type.extension_name)


class BaseGeometryType(pa.ExtensionType):
    _storage_type: pa.DataType
    _extension_name: str

    def __init__(self, crs=None):
        # attributes need to be set first before calling
        # super init (as that calls serialize)
        self._crs = crs
        pa.ExtensionType.__init__(self, self._storage_type, self._extension_name)

    @property
    def crs(self):
        return self._crs

    def __arrow_ext_serialize__(self):
        return b"CREATED"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        # TODO ignore serialized metadata for now
        # serialized = serialized.decode()
        # assert serialized.startswith("crs=")
        # crs = serialized.split('=')[1]
        # if crs == "":
        #     crs = None
        return cls()

    def __arrow_ext_class__(self):
        return ArrowGeometryArray


_point_storage_type = pa.list_(pa.field("xy", pa.float64()), 2)


class PointGeometryType(BaseGeometryType):
    _storage_type = _point_storage_type
    _extension_name = "geoarrow.point"


_point_type = PointGeometryType()

_linestring_storage_type = pa.list_(pa.field("vertices", _point_type))
_polygon_storage_type = pa.list_(
    pa.field("rings", pa.list_(pa.field("vertices", _point_storage_type)))
)


class LineStringGeometryType(BaseGeometryType):
    _storage_type = _linestring_storage_type
    _extension_name = "geoarrow.linestring"


class PolygonGeometryType(BaseGeometryType):
    _storage_type = _polygon_storage_type
    _extension_name = "geoarrow.polygon"


_linestring_type = LineStringGeometryType()
_polygon_type = PolygonGeometryType()


_multipoint_storage_type = pa.list_(pa.field("points", _point_type))
_multilinestring_storage_type = pa.list_(pa.field("linestrings", _linestring_type))
_multipolygon_storage_type = pa.list_(pa.field("polygons", _polygon_type))


class MultiPointGeometryType(BaseGeometryType):
    _storage_type = _multipoint_storage_type
    _extension_name = "geoarrow.multipoint"


class MultiLineStringGeometryType(BaseGeometryType):
    _storage_type = _multilinestring_storage_type
    _extension_name = "geoarrow.multilinestring"


class MultiPolygonGeometryType(BaseGeometryType):
    _storage_type = _multipolygon_storage_type
    _extension_name = "geoarrow.multipolygon"


_multipoint_type = MultiPointGeometryType()
_multilinestring_type = MultiLineStringGeometryType()
_multipolygon_type = MultiPolygonGeometryType()


def register_geometry_extension_types():
    for geom_type in [
        _point_type,
        _linestring_type,
        _polygon_type,
        _multipoint_type,
        _multilinestring_type,
        _multipolygon_type,
    ]:
        pa.register_extension_type(geom_type)


def unregister_geometry_extension_types():
    for geom_name in [
        "geoarrow.point",
        "geoarrow.linestring",
        "geoarrow.polygon",
        "geoarrow.multipoint",
        "geoarrow.multilinestring",
        "geoarrow.multipolygon",
    ]:
        pa.unregister_extension_type(geom_name)


def construct_geometry_array(arr):
    typ, coords, offsets = get_flat_coords_offset_arrays(arr)

    if typ == "point":
        parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        return pa.ExtensionArray.from_storage(PointGeometryType(), parr)

    elif typ == "linestring":
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        parr = pa.ListArray.from_arrays(pa.array(offsets), _parr)
        return pa.ExtensionArray.from_storage(LineStringGeometryType(), parr)

    elif typ == "polygon":
        offsets1, offsets2 = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        _parr1 = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        parr = pa.ListArray.from_arrays(pa.array(offsets2), _parr1)
        # Got a TypeError when trying to cast to the extension type, but I don't really
        # need an extension type for now.
        # TypeError: Incompatible storage type list<item: list<item: fixed_size_list<item: double>[2]>> for extension type extension<geoarrow.polygon<PolygonGeometryType>>
        # return pa.ExtensionArray.from_storage(PolygonGeometryType(), parr)
        return parr

    elif typ == "multipoint":
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        parr = pa.ListArray.from_arrays(pa.array(offsets), _parr)
        return pa.ExtensionArray.from_storage(MultiPointGeometryType(), parr)

    elif typ == "multilinestring":
        offsets1, offsets2 = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        _parr1 = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        parr = pa.ListArray.from_arrays(pa.array(offsets2), _parr1)
        return pa.ExtensionArray.from_storage(MultiLineStringGeometryType(), parr)

    elif typ == "multipolygon":
        offsets1, offsets2, offsets3 = offsets
        _parr = pa.FixedSizeListArray.from_arrays(coords, 2)
        _parr1 = pa.ListArray.from_arrays(pa.array(offsets1), _parr)
        _parr2 = pa.ListArray.from_arrays(pa.array(offsets2), _parr1)
        parr = pa.ListArray.from_arrays(pa.array(offsets3), _parr2)
        return pa.ExtensionArray.from_storage(MultiPolygonGeometryType(), parr)

    else:
        raise ValueError("wrong type ", typ)


def construct_numpy_array(arr, extension_name):

    if extension_name == "geoarrow.point":
        coords = np.asarray(arr.values)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("point", coords.copy(), None)

    elif extension_name == "geoarrow.linestring":
        coords = np.asarray(arr.values.values)
        offsets = np.asarray(arr.offsets)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("linestring", coords.copy(), offsets)

    elif extension_name == "geoarrow.polygon":
        coords = np.asarray(arr.values.values.values)
        offsets2 = np.asarray(arr.offsets)
        offsets1 = np.asarray(arr.values.offsets)
        offsets = (offsets1, offsets2)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("polygon", coords.copy(), offsets)

    elif extension_name == "geoarrow.multipoint":
        coords = np.asarray(arr.values.values)
        offsets = np.asarray(arr.offsets)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("multipoint", coords.copy(), offsets)

    elif extension_name == "geoarrow.multilinestring":
        coords = np.asarray(arr.values.values.values)
        offsets2 = np.asarray(arr.offsets)
        offsets1 = np.asarray(arr.values.offsets)
        offsets = (offsets1, offsets2)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("multilinestring", coords.copy(), offsets)

    elif extension_name == "geoarrow.multipolygon":
        coords = np.asarray(arr.values.values.values.values)
        offsets3 = np.asarray(arr.offsets)
        offsets2 = np.asarray(arr.values.offsets)
        offsets1 = np.asarray(arr.values.values.offsets)
        offsets = (offsets1, offsets2, offsets3)
        # TODO copy is needed because of read-only memoryview bug in pygeos
        return get_geometries_from_flatcoords("multipolygon", coords.copy(), offsets)

    else:
        raise ValueError(extension_name)
