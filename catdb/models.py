"""
This file can be used to insert a set of
article_categories.
"""

__all__ = ['ArticleCategory', 'CategoryCategory', 'CategoryLabel', 'database_proxy']

import peewee
from peewee import IntegerField, CharField, PrimaryKeyField
from peewee import Model
from playhouse.proxy import Proxy

ARTICLE_MAX_LENGTH = 200
CATEGORY_MAX_LENGTH = 200
LABEL_MAX_LENGTH = 200

database_proxy = Proxy()  # Create a proxy for our db.

class BaseModel(Model):
    class Meta:
        database = database_proxy  # Use proxy for our DB.

class ArticleCategory(BaseModel):
    id = PrimaryKeyField()
    article = CharField(index=True, max_length=ARTICLE_MAX_LENGTH)
    category = CharField(index=True, max_length=CATEGORY_MAX_LENGTH)

    class Meta:
        db_table='article_categories'

class CategoryCategory(BaseModel):
    id = PrimaryKeyField()
    narrower = CharField(index=True, max_length=CATEGORY_MAX_LENGTH)
    broader = CharField(index=True, max_length=CATEGORY_MAX_LENGTH)

    class Meta:
        db_table='category_categories'

class CategoryLabel(BaseModel):
    id = PrimaryKeyField()
    category = CharField(index=True, max_length=CATEGORY_MAX_LENGTH)
    label = CharField(index=True, max_length=LABEL_MAX_LENGTH)

    class Meta:
        db_table='category_labels'
