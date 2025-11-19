
  
    # models/silver/users_cleaned.py
import dbt
import polars as pl

# Define the model function. dbt calls this function.
def model(dbt, session):
    """
    Reads the Bronze layer data, applies cleaning and standardization using Polars,
    and returns the resulting cleaned DataFrame.
    """

    # 1. Read the Bronze layer data (which is the 'raw_data' view/table)
    # dbt automatically handles the connection (DuckDB session) to convert the
    # reference into a Polars DataFrame.
    bronze_df = dbt.ref("users_old")

    # Ensure the DataFrame is a Polars DataFrame for cleaning
    df = pl.DataFrame(bronze_df)

    # 2. Apply Polars Cleaning and Standardization Expressions
    cleaned_df = df.with_columns(
        # Convert 'id' to integer, handling nulls if present
        pl.col("id").cast(pl.Int32, strict=False),

        # Clean and standardize names (e.g., trim whitespace, title case)
        pl.col("first_name").str.strip_chars().str.to_titlecase(),
    ).select(
        "id",
        "first_name",
    )

    # 3. Return the cleaned Polars DataFrame
    return cleaned_df


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"users_old": "\"dev\".\"main\".\"users_old\""}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "dev"
    schema = "main"
    identifier = "users_new"
    
    def __repr__(self):
        return '"dev"."main"."users_new"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------




def materialize(df, con):
    try:
        import pyarrow
        pyarrow_available = True
    except ImportError:
        pyarrow_available = False
    finally:
        if pyarrow_available and isinstance(df, pyarrow.Table):
            # https://github.com/duckdb/duckdb/issues/6584
            import pyarrow.dataset
    tmp_name = '__dbt_python_model_df_' + 'users_new__dbt_tmp'
    con.register(tmp_name, df)
    con.execute('create table "dev"."main"."users_new__dbt_tmp" as select * from ' + tmp_name)
    con.unregister(tmp_name)

  