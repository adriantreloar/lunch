from lunch.examples.insert_dimension_data import insert_dimension_data
from lunch.examples.save_fact import save_fact

async def save_fact_data():

    await insert_dimension_data()
    await save_fact()

    # TODO save_fact not actually creating the fact table...

    # TODO, change insert_dimension_data to insert some of the other dimensions

    # Create some fact data

    # append it to f_sales


