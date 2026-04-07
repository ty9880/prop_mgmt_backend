from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel
from datetime import date

app = FastAPI()

PROJECT_ID = "mgmt54500-sp26-dev-tucci"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties

@app.get("/properties/{property_id}")
def get_property_by_id(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a single property by ID.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
        property_record = [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not property_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    return property_record[0]

@app.get("/properties/{property_id}/income")
def get_income_by_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all income records for a specific property.
    """
    query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date, income_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
        income_records = [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not income_records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No income records found for property ID {property_id}"
        )

    return income_records

class IncomeCreate(BaseModel):
    amount: float
    date: date
    description: str | None = None


@app.post("/income/{property_id}", status_code=status.HTTP_201_CREATED)
def create_income_record(
    property_id: int,
    income: IncomeCreate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Creates a new income record for a specific property.
    """

    # Check that the property exists
    property_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    property_job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        property_results = list(bq.query(property_query, job_config=property_job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed while checking property: {str(e)}"
        )

    if not property_results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    # Generate next income_id
    next_id_query = f"""
        SELECT COALESCE(MAX(income_id), 0) + 1 AS next_income_id
        FROM `{PROJECT_ID}.{DATASET}.income`
    """

    try:
        next_id_result = list(bq.query(next_id_query).result())
        next_income_id = next_id_result[0]["next_income_id"]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed while generating income_id: {str(e)}"
        )

    # Insert new income record
    rows_to_insert = [
        {
            "income_id": next_income_id,
            "property_id": property_id,
            "amount": income.amount,
            "date": income.date.isoformat(),
            "description": income.description
        }
    ]

    table_id = f"{PROJECT_ID}.{DATASET}.income"

    try:
        errors = bq.insert_rows_json(table_id, rows_to_insert)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )

    if errors:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed to insert income record", "errors": errors}
        )

    return {
        "message": "Income record created successfully",
        "income_id": next_income_id,
        "property_id": property_id,
        "amount": income.amount,
        "date": income.date,
        "description": income.description
    }

@app.get("/properties/{property_id}/expenses")
def get_expenses_by_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all expense records for a specific property.
    """
    query = f"""
        SELECT
            expense_id,
            property_id,
            amount,
            date,
            category,
            vendor,
            description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY date, expense_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
        expense_records = [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not expense_records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No expense records found for property ID {property_id}"
        )

    return expense_records

class ExpenseCreate(BaseModel):
    amount: float
    date: date
    category: str
    vendor: str | None = None
    description: str | None = None


@app.post("/expenses/{property_id}", status_code=status.HTTP_201_CREATED)
def create_expense_record(
    property_id: int,
    expense: ExpenseCreate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Creates a new expense record for a specific property.
    """

    # Check that property exists
    property_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    property_job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        property_results = list(bq.query(property_query, job_config=property_job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed while checking property: {str(e)}"
        )

    if not property_results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    # Generate next expense_id
    next_id_query = f"""
        SELECT COALESCE(MAX(expense_id), 0) + 1 AS next_expense_id
        FROM `{PROJECT_ID}.{DATASET}.expenses`
    """

    try:
        next_id_result = list(bq.query(next_id_query).result())
        next_expense_id = next_id_result[0]["next_expense_id"]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed while generating expense_id: {str(e)}"
        )

    # Insert new expense
    rows_to_insert = [
        {
            "expense_id": next_expense_id,
            "property_id": property_id,
            "amount": expense.amount,
            "date": expense.date.isoformat(),
            "category": expense.category,
            "vendor": expense.vendor,
            "description": expense.description
        }
    ]

    table_id = f"{PROJECT_ID}.{DATASET}.expenses"

    try:
        errors = bq.insert_rows_json(table_id, rows_to_insert)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )

    if errors:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed to insert expense record", "errors": errors}
        )

    return {
        "message": "Expense record created successfully",
        "expense_id": next_expense_id,
        "property_id": property_id,
        "amount": expense.amount,
        "date": expense.date,
        "category": expense.category,
        "vendor": expense.vendor,
        "description": expense.description
    }

@app.get("/properties/{property_id}/income/total")
def get_total_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns the total income for a specific property.
    """
    query = f"""
        SELECT COALESCE(SUM(amount), 0) AS total_income
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        result = list(bq.query(query, job_config=job_config).result())
        total_income = result[0]["total_income"]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return {
        "property_id": property_id,
        "total_income": total_income
    }

@app.get("/properties/{property_id}/expenses/total")
def get_total_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns the total expenses for a specific property.
    """
    query = f"""
        SELECT COALESCE(SUM(amount), 0) AS total_expenses
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        result = list(bq.query(query, job_config=job_config).result())
        total_expenses = result[0]["total_expenses"]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return {
        "property_id": property_id,
        "total_expenses": total_expenses
    }

@app.get("/properties/{property_id}/profit")
def get_property_profit(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns the profit for a property (total income - total expenses).
    """
    query = f"""
        SELECT
            COALESCE(i.total_income, 0) - COALESCE(e.total_expenses, 0) AS profit
        FROM
            (SELECT SUM(amount) AS total_income
             FROM `{PROJECT_ID}.{DATASET}.income`
             WHERE property_id = @property_id) i,
            (SELECT SUM(amount) AS total_expenses
             FROM `{PROJECT_ID}.{DATASET}.expenses`
             WHERE property_id = @property_id) e
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        result = list(bq.query(query, job_config=job_config).result())
        profit = result[0]["profit"]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return {
        "property_id": property_id,
        "profit": profit
    }

class IncomeUpdate(BaseModel):
    amount: float
    date: date
    description: str | None = None


@app.put("/income/{income_id}")
def update_income_record(
    income_id: int,
    income: IncomeUpdate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Updates an existing income record.
    """

    # Check if record exists
    check_query = f"""
        SELECT income_id
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE income_id = @income_id
    """

    check_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("income_id", "INT64", income_id)
        ]
    )

    try:
        existing = list(bq.query(check_query, job_config=check_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Income record with ID {income_id} not found"
        )

    # Update query
    update_query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.income`
        SET
            amount = @amount,
            date = @date,
            description = @description
        WHERE income_id = @income_id
    """

    update_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("amount", "FLOAT64", income.amount),
            bigquery.ScalarQueryParameter("date", "DATE", income.date),
            bigquery.ScalarQueryParameter("description", "STRING", income.description),
            bigquery.ScalarQueryParameter("income_id", "INT64", income_id)
        ]
    )

    try:
        bq.query(update_query, job_config=update_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Update failed: {str(e)}"
        )

    return {
        "message": "Income record updated successfully",
        "income_id": income_id,
        "amount": income.amount,
        "date": income.date,
        "description": income.description
    }