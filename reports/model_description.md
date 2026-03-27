# Star Schema Data Model

## ERD Diagram
```mermaid
erDiagram
    fact_sales {
        int sale_id PK
        int invoice_id
        int product_id FK
        int customer_id FK
        int location_id FK
        int date_id FK
        int quantity
        float price
        float total_revenue
    }
    dim_product {
        int product_id PK
        string product_name
    }
    dim_customer {
        int customer_id PK
        string customer_key
    }
    dim_location {
        int location_id PK
        string country
    }
    dim_date {
        int date_id PK
        date full_date
        int year
        int month
        string month_name
        string day_of_week
    }
    fact_sales }O--|| dim_product : references
    fact_sales }O--|| dim_customer : references
    fact_sales }O--|| dim_location : references
    fact_sales }O--|| dim_date : references
```

## Table Descriptions
1. **dim_product**: Contains unique products parsed from raw data.
2. **dim_customer**: Links surrogate customer_id to raw source customer_key.
3. **dim_location**: Maps country strings to surrogate location_id.
4. **dim_date**: A complete date dimension for the year 2023 with rich date attributes.
5. **fact_sales**: Transactional data storing quantities and revenues matched to dimension keys.
