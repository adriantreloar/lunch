from lunch.examples.insert_dimension_data import save_dimension
from lunch.examples.save_fact import save_fact

async def save_fact_data():

    await save_dimension()
    await save_fact()

