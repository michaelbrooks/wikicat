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

    @classmethod
    def generate_batch_insert(cls, dictionaries):
        """
        Generates a bulk insert statement a list of dictionaries
        representing model data.
        :param dictionaries:
        :return:
        """

        if len(dictionaries) == 0:
            return None

        # get an example dictionary
        example = dictionaries[0]

        quote_char = cls._meta.database.quote_char
        interpolation = cls._meta.database.interpolation

        parts = ['INSERT INTO %s%s%s' % (quote_char, cls._meta.db_table, quote_char)]
        fields = [f for f in cls._meta.fields if f in example]

        parts.append("(")
        parts.append(",".join('%s%s%s' % (quote_char, f, quote_char) for f in fields))
        parts.append(")")

        parts.append("VALUES")

        params = []

        settings = []
        for d in dictionaries:

            values = [d[f] for f in fields]
            placeholder = ",".join(interpolation for v in values)
            settings.append("(%s)" % placeholder)
            params.extend(values)

        parts.append(",".join(settings))
        sql = " ".join(parts)

        return sql, params

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
