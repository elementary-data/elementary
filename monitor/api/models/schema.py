from pydantic import BaseModel


class ModelSchema(BaseModel):
    pass


class NormalizedModelSchema(ModelSchema):
    pass


class SourceSchema(BaseModel):
    pass


class NormalizedSourceSchema(SourceSchema):
    pass
