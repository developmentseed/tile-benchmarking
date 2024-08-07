from test import Test, timer_decorator
from titiler.cmr.backend import CMRBackend
from titiler.cmr.reader import ZarrReader
from rio_tiler.models import ImageData


"""Create map tile."""


def tile(
    tile_x: int,
    tile_y: int,
    tile_z: int,
    *,
    concept_id: str,
    cmr_query: dict,
    reader_options: dict,
    **kwargs,
) -> tuple[ImageData, list[str]]:
    backend = CMRBackend(reader=ZarrReader, reader_options=reader_options)
    cmr_query["concept_id"] = concept_id

    return backend.tile(tile_x, tile_y, tile_z, cmr_query=cmr_query, **kwargs)


class CMRTileTest(Test):
    def __init__(self, *, cmr_query, **kwargs):
        super().__init__(**kwargs)
        self.cmr_query = cmr_query
        self.reader_options = {"variable": self.variable}

    def generate_arguments(self, batch_size: int = 1, zoom: int = 0):
        return [self.generate_random_tile(zoom) for i in range(batch_size)]

    @timer_decorator
    def run(self, xyz_tile: tuple[int, int, int], **kwargs):
        return tile(
            *xyz_tile,
            concept_id=self.dataset_id,
            cmr_query=self.cmr_query,
            reader_options=self.reader_options,
            **kwargs,
        )
