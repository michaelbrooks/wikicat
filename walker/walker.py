import web
import json
from web import form

from catdb.models import BaseModel, CategoryStats, Category, CategoryCategory, Article, DataSetVersion, ArticleCategory
from peewee import fn

def jsonhandler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    elif isinstance(obj, BaseModel):
        return obj._data
    else:
        raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj))


def category_url(name):
    return "/walker/category/%s" % Category.short_name(name)


def article_url(name):
    return "/walker/article/%s" % name

class base_controller(object):
    render = None

class article_api(base_controller):
    def GET(self, version_id, article_id):
        version = DataSetVersion.select() \
            .where(DataSetVersion.id == version_id) \
            .first()

        article = Article.select() \
            .where(Article.id == article_id) \
            .first()

        #subcategories = category.get_children(version=version)
        subcategories = Category.select(Category,
                                        CategoryStats.subcategories,
                                        CategoryStats.supercategories,
                                        CategoryStats.articles) \
            .join(ArticleCategory, on=ArticleCategory.category) \
            .where(ArticleCategory.article == article) \
            .where(ArticleCategory.version == version) \
            .join(CategoryStats, on=CategoryStats.category == Category.id) \
            .where(CategoryStats.version == version) \
            .dicts()

        #supercategories = category.get_parents(version=version)
        supercategories = Category.select(Category,
                                          CategoryStats.subcategories,
                                          CategoryStats.supercategories,
                                          CategoryStats.articles) \
            .join(ArticleCategory, on=ArticleCategory.category) \
            .where(ArticleCategory.article == article) \
            .where(ArticleCategory.version == version) \
            .join(CategoryStats, on=CategoryStats.category == Category.id) \
            .where(CategoryStats.version == version) \
            .dicts()

        process_categories(supercategories)
        process_categories(subcategories)

        response = {
            'version': version._data,
            'article': article._data,
            'articles': list(),
            'subcategories': list(subcategories),
            'supercategories': list(supercategories),
        }

        web.header('Content-Type', 'application/json')
        return json.dumps(response, default=jsonhandler)


def process_categories(categories):
    for cat in categories:
        cat['url'] = category_url(cat['name'])
        cat['short_name'] = Category.short_name(cat['name'])

def process_articles(articles):
    for art in articles:
        art['url'] = article_url(art['name'])

class category_api(base_controller):
    def GET(self, version_id, category_id):
        version = DataSetVersion.select() \
            .where(DataSetVersion.id == version_id) \
            .first()

        #version.date = version.date.strftime('%Y-%m-%dT%H:%M:%S')

        category = Category.select() \
            .where(Category.id == category_id) \
            .first()

        category.short_name = Category.short_name(category.name)

        articles = category.get_articles(version=version).dicts()

        #subcategories = category.get_children(version=version)
        subcategories = Category.select(Category,
                                        CategoryStats.subcategories,
                                        CategoryStats.supercategories,
                                        CategoryStats.articles) \
            .join(CategoryCategory, on=CategoryCategory.narrower) \
            .where(CategoryCategory.broader == category) \
            .where(CategoryCategory.version == version) \
            .join(CategoryStats, on=CategoryStats.category == Category.id) \
            .where(CategoryStats.version == version) \
            .dicts()

        #supercategories = category.get_parents(version=version)
        supercategories = Category.select(Category,
                                          CategoryStats.subcategories,
                                          CategoryStats.supercategories,
                                          CategoryStats.articles) \
            .join(CategoryCategory, on=CategoryCategory.broader) \
            .where(CategoryCategory.narrower == category) \
            .where(CategoryCategory.version == version) \
            .join(CategoryStats, on=CategoryStats.category == Category.id) \
            .where(CategoryStats.version == version) \
            .dicts()

        process_categories(supercategories)
        process_categories(subcategories)
        process_articles(articles)

        response = {
            'version': version._data,
            'category': category._data,
            'articles': list(articles),
            'subcategories': list(subcategories),
            'supercategories': list(supercategories),
        }

        web.header('Content-Type', 'application/json')
        return json.dumps(response, default=jsonhandler)


class category(base_controller):
    def GET(self, category_name):
        category_name = Category.long_name(category_name)

        category = Category.select().where(Category.name == category_name) \
            .first()

        category.short_name = Category.short_name(category.name)

        versions = DataSetVersion.select() \
            .order_by(DataSetVersion.date)

        return self.render.category_walker(category=category,
                                      versions=versions)


class article(base_controller):
    def GET(self, article_name):
        article = Article.select().where(Article.name == article_name) \
            .first()

        versions = DataSetVersion.select() \
            .order_by(DataSetVersion.date)

        return self.render.article_walker(article=article,
                                     versions=versions)


class Searchbox(form.Input):
    def get_type(self):
        return 'search'


search_form = form.Form(
    Searchbox("search", form.notnull,
              description="Search",
              placeholder="Search",
              class_="form-control"),
    form.Button("submit",
                type="submit",
                html="Submit",
                class_="btn btn-primary")
)


class index(base_controller):
    def GET(self):
        form = search_form()

        # check for a search parameter
        search = ""
        input = web.input()
        if input.get('q'):
            search = input.q
            form.search.value = search

        return self.render.search(form=form, search=search)

    def POST(self):
        form = search_form()
        if not form.validates():
            raise web.badrequest()

        search = str(form.d.search)
        like_search = '%%%s%%' % search.lower()

        article_sum = fn.Sum(CategoryStats.articles)
        subcategory_sum = fn.Sum(CategoryStats.subcategories)
        categories = Category.select(Category,
                                     fn.Count(fn.Distinct(CategoryStats.id)).alias('versions'),
                                     subcategory_sum.alias('subcategories'),
                                     article_sum.alias('articles'),
                                     fn.Sum(CategoryStats.supercategories).alias('supercategories')) \
            .where(fn.Lower(Category.name) % like_search) \
            .join(CategoryStats, on=CategoryStats.category == Category.id) \
            .group_by(Category) \
            .order_by((subcategory_sum + article_sum).desc()) \
            .limit(100)\
            .dicts()

        version_count = fn.Count(fn.Distinct(ArticleCategory.version))
        articles = Article.select(Article,
                                  version_count.alias('versions'),
                                  fn.Count(fn.Distinct(ArticleCategory.id)).alias('categories')) \
            .where(fn.Lower(Article.name) % like_search) \
            .join(ArticleCategory, on=ArticleCategory.article == Article.id) \
            .group_by(Article) \
            .order_by(version_count.desc()) \
            .limit(100)\
            .dicts()

        process_categories(categories)
        process_articles(articles)

        response = {
            'search': search,
            'categories': list(categories),
            'articles': list(articles)
        }

        web.header('Content-Type', 'application/json')
        return json.dumps(response, default=jsonhandler)


urls = (
    '/walker/article/(.+)', article,
    '/api/article/(\d+)/(\d+)', article_api,
    '/walker/category/(.+)', category,
    '/api/category/(\d+)/(\d+)', category_api,
    '/walker', index
)


def configure_templates(base_dir):
    import os
    base_controller.render = web.template.render(os.path.join(base_dir, 'templates'), base='page')