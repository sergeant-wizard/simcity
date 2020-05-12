import collections
import dataclasses
import enum
import functools
import io
import typing

import pandas
import requests


class Table(enum.Enum):
    Products = 0
    Recipes = 1

    def load_csv(self) -> pandas.DataFrame:
        base_url = (
            'https://docs.google.com/spreadsheets/d/e/'
            '2PACX-1vQk9rrd2tXpNTLKiTSyA2AnXwBSWB48D467wpjsLV_bfrQ6P3gz8SGHyBsAC-dluS6IOtwHzyr74ymf/pub'  # NOQA
        )
        response = requests.get(base_url, params={
            'single': 'true',
            'output': 'csv',
            'gid': {
                Table.Products: '0',
                Table.Recipes: '475544489',
            }[self],
        })
        assert response.ok
        return pandas.read_csv(io.StringIO(response.text))

    def itertuples(self) -> typing.Iterable:
        return self.load_csv().itertuples(index=False, name='Table')


# is there a better way to annotate dataclasses?
class Model:
    def __init__(self, adict):
        pass


@dataclasses.dataclass
class Product(Model):
    name: str
    price: int
    duration: float
    source: str

    @classmethod
    def from_tuple(cls, atuple) -> 'Product':
        return cls(**atuple._asdict())


ProductsDict = typing.Dict[str, Product]


@dataclasses.dataclass
class RecipeEntry(Model):
    product_name: str
    ingredient: Product
    quantity: int

    @classmethod
    def from_tuple(
        cls,
        atuple,  # pandas named tuple cannot be type annotated?
        products: ProductsDict,
    ) -> 'RecipeEntry':
        return cls(**{
            **atuple._asdict(),
            **{'ingredient': products[atuple.ingredient]},
        })


@dataclasses.dataclass
class Recipe:
    quantity: int
    ingredient_name: str


RecipesList = typing.List[Recipe]
RecipesDict = typing.DefaultDict[str, RecipesList]


def traverse(
    adict: RecipesDict,
    product_name: str,
    quantity: int,
) -> RecipesList:
    ret: RecipesList = []
    for recipe in adict[product_name]:
        ret.append(Recipe(
            ingredient_name=recipe.ingredient_name,
            quantity=quantity*recipe.quantity,
        ))
        if recipe.ingredient_name in adict.keys():
            ret += traverse(
                adict, recipe.ingredient_name, quantity * recipe.quantity,
            )
    return ret


def make_recipe_dict(products: ProductsDict) -> RecipesDict:
    recipes = [
        RecipeEntry.from_tuple(atuple, products)
        for atuple in Table.Recipes.itertuples()
    ]
    recipes_dict: RecipesDict = collections.defaultdict(list)
    for recipe_entry in recipes:
        recipes_dict[recipe_entry.product_name].append(Recipe(
            ingredient_name=recipe_entry.ingredient.name,
            quantity=recipe_entry.quantity,
        ))
    return recipes_dict


def make_recipe_df(products: ProductsDict) -> pandas.DataFrame:
    recipes_dict = make_recipe_dict(products)
    return pandas.concat({
        product_name: pandas.DataFrame(map(dataclasses.asdict, alist))
        for product_name, alist in recipes_dict.items()
    }, names=['product_name']).reset_index(1, drop=True)


def make_products_df(products_dict: ProductsDict) -> pandas.DataFrame:
    products = pandas.concat({
        product_name: pandas.Series(dataclasses.asdict(product))
        for product_name, product in products_dict.items()
    }).unstack(1).drop('name', axis=1)
    products['duration'] = products.duration.astype(float)
    return products


@functools.lru_cache(0)
def make_products_dict() -> ProductsDict:
    return {
        atuple.name: Product.from_tuple(atuple)
        for atuple in Table.Products.itertuples()
    }


def make_df() -> typing.Tuple[pandas.DataFrame, pandas.DataFrame]:
    products_dict = make_products_dict()
    recipe = make_recipe_df(products_dict)
    products = make_products_df(products_dict)

    return recipe, products


def added_value() -> pandas.DataFrame:
    recipe, products = make_df()
    df = recipe.join(products, on='ingredient_name').reset_index()

    def scale_price(row):
        return row.quantity * row.price

    df['ingredients_price'] = df.apply(scale_price, axis=1)

    products.query('source != "factory"', inplace=True)
    products['ingredients_price'] = df.groupby('product_name').sum(
    ).ingredients_price
    products['profit'] = products.price - products.ingredients_price
    products['rate'] = products.profit / products.duration
    max_rate_per_source = products.groupby('source').rate.transform(max)
    return products.loc[products.rate == max_rate_per_source]


def make_flat_recipe_df(
    products_dict: pandas.DataFrame,
) -> pandas.DataFrame:
    recipes_dict = make_recipe_dict(products_dict)
    flat_list = {
        product_name: traverse(recipes_dict, product_name, 1)
        for product_name in recipes_dict.keys()
    }
    return pandas.concat({
        product_name: pandas.DataFrame(map(dataclasses.asdict, recipe_list))
        for product_name, recipe_list in flat_list.items()
    }, names=['product_name'])


def bottleneck() -> pandas.DataFrame:
    products_dict = make_products_dict()
    products_df = make_products_df(products_dict)
    recipe_df = make_flat_recipe_df(products_dict)
    df = recipe_df.join(
        products_df, on='ingredient_name',
    ).query('source != "factory"')

    df['scaled_duration'] = df.quantity * df.duration
    total_duration = df.groupby(
        ['product_name', 'source']
    ).sum()
    max_duration = total_duration.groupby(
        'product_name'
    ).transform(max).scaled_duration
    ret = total_duration.loc[
        total_duration.scaled_duration == max_duration
    ].copy().reset_index('source')
    ret['sell_price'] = products_df.price
    return ret


def factory() -> None:
    products_dict = make_products_dict()
    recipe_df = make_flat_recipe_df(products_dict)
    products_df = make_products_df(products_dict)
    df = recipe_df.join(products_df, on='ingredient_name')

    target_products = [
        'donuts', 'couch', 'lawn mower'
    ]
    print(df.loc[target_products].query('source=="factory"').groupby(
        'ingredient_name').sum())


if __name__ == '__main__':
    factory()
    bottleneck().to_csv('bottleneck.csv')
    print(added_value())
