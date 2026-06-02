# Alasco FinCon API Overview

> Documentation: https://developer.alasco.de/fincon.html
> OpenAPI Spec: `./openapi/openapi.json`

---

## API Basics

### Authentication

The API uses token-based authentication with two HTTP headers:

| Header | Environment Variable | Description |
|--------|---------------------|-------------|
| `X-API-TOKEN` | `ALASCO_API_TOKEN` | API token (64 characters) |
| `X-API-KEY` | `ALASCO_API_KEY` | API key (40 characters) |

Both headers are required for all requests.

### Base URL

```
https://api.alasco.de/v1/
```

### Response Format (JSON:API)

All responses follow the JSON:API specification:

```json
{
  "data": [
    {
      "id": "uuid-string",
      "type": "resource_type",
      "attributes": { ... },
      "relationships": { ... }
    }
  ],
  "included": [ ... ],  // Related resources (when using ?include=)
  "links": {
    "next": "...",      // Pagination
    "prev": "..."
  },
  "errors": [ ... ]     // Only present on errors
}
```

### Pagination

Responses are paginated. Use `links.next` to fetch subsequent pages.

### Including Related Resources

Use the `?include=` query parameter to embed related resources in the response:
```
GET /contracts/?include=contractor,contract_unit
```

### Filtering

Filter parameters follow the format `filter[attribute.operation]`:

```
GET /projects/?filter[property.in]=uuid1,uuid2
GET /contracts/?filter[contract_unit.in]=uuid1,uuid2
```

Operations:
- `in`: Match any of the provided values (comma-separated)
- `not_in`: Exclude these values
- `exact`: Exact match
- `contains`: Substring match
- `less_than_equal`, `greater_than_equal`, `range`: Numeric/date comparisons
- `is`: Boolean check

---

## Entity Hierarchy

```
Property
  │
  └── Project
        │
        ├── Cost Element (hierarchical via parent field)
        │
        └── Contract Unit
              │
              └── Contract
                    │
                    └── Contractor (referenced)
```

### Key Relationships

| Entity | References | Via Field |
|--------|------------|-----------|
| Project | Property | `property` |
| Contract Unit | Project | `project` |
| Contract | Contract Unit | `contract_unit` |
| Contract | Contractor | `contractor` |
| Cost Element | Parent Cost Element | `parent` |
| Cost Element | Project | via `cost_element_tree_template` |

---

## Core Endpoints

### Properties

```
GET /properties/                      # List all properties
GET /properties/{id}/                 # Get property details
GET /properties/{id}/projects/        # Get projects for property
```

**PropertyAttributes:**
- `name`: string
- `address`, `city`, `zip_code`, `country`: string
- `description`: string
- `date_created`: string (ISO date)

### Projects

```
GET /projects/                        # List all projects
GET /projects/{id}/                   # Get project details
```

**Query Parameters:**
- `filter[property.in]`: Filter by property UUID(s)

**ProjectAttributes:**
- `name`: string
- `identifier`: string (human-readable project code)
- `property`: UUID (reference to property)
- `is_active`: boolean
- `description`: string
- `cost_element_tree_template`: UUID or null
- `default_contracting_entity`: UUID
- `default_tax_rate`: string (decimal)
- Area fields: `gross_floor_area`, `rental_area`, `residential_area`, etc.
- Dates: `estimated_start_date`, `estimated_end_date`

### Contract Units

```
GET /contract_units/                  # List all contract units
GET /contract_units/{id}/             # Get contract unit details
GET /contract_units/{id}/contracts/   # Get contracts for unit
```

**Query Parameters:**
- `filter[project.in]`: Filter by project UUID(s)

**ContractUnitAttributes:**
- `name`: string (e.g., "4-731-1 Architektenleistungen")
- `description`: string
- `project`: UUID (reference to project)
- `state`: enum (ContractUnitState)

### Contracts

```
GET /contracts/                       # List all contracts
GET /contracts/{id}/                  # Get contract details
```

**Query Parameters:**
- `filter[contract_unit.in]`: Filter by contract unit UUID(s)

**ReadContractAttributesFincon:**
- `name`: string
- `contract_number`: string (human-readable, may be duplicate or empty)
- `contract_unit`: UUID (reference)
- `contractor`: UUID or null (reference)
- `contracting_entity`: UUID (reference)
- `contract_type`: enum (ContractType)
- `status`: enum (ContractStatus)
- `amount`: object with `net`, `gross` fields
- `reserve_amount`: object with `net`, `gross` fields
- `date_contract`: string (ISO date) or null
- `date_created`: string (ISO date)
- `is_reverse_charge`: boolean

### Contractors

```
GET /contractors/                     # List all contractors
GET /contractors/{id}/                # Get contractor details
```

**ContractorAttributes:**
- `name`: string (company name)
- Contact info, tax info, etc.

### Cost Elements

```
GET /cost_elements/                   # List all cost elements
GET /cost_elements/{id}/              # Get cost element details
```

**CostElementAttributes:**
- `name`: string (e.g., "4 - Planung")
- `description`: string
- `parent`: UUID or null (self-reference for hierarchy)
- `cost_category`: enum or null
- `cost_element_tree_template`: UUID or null

---

## Reporting Endpoints

These endpoints return **financial data** aggregated at different levels.

### Contract Financials

```
GET /reporting/contracts/
```

**Query Parameters:**
- `filter[project.in]`: Filter by project UUID(s)
- `filter[contract_unit.in]`: Filter by contract unit UUID(s)

**ContractFinancialsAttributes:**
- `name`: string
- `project`: UUID
- `contract_unit`: UUID

**Financial Fields** (all are objects with `net` and `gross` sub-fields):
- `initial_budget`: Starting budget
- `current_budget`: Current budget after adjustments
- `main_contract_amount`: Base contract value
- `change_orders_amount`: Approved change orders
- `open_change_orders_amount`: Pending change orders (weighted)
- `open_change_orders_amount_unweighted`: Pending change orders (unweighted)
- `contract_cost_forecast_worst_case`: Worst-case cost forecast
- `contract_cost_forecast_real_case`: Realistic cost forecast
- `reserves_amount`: Provisions/reserves
- `contract_budget_deviation_worst_case`: Budget deviation (worst-case)
- `approved_amount`: Amounts approved for payment
- `paid_amount`: Amounts actually paid
- `contractual_retentions`: Contractual withholdings
- `manual_retentions`: Manual withholdings
- And more...

### Contract Unit Financials

```
GET /reporting/contract_units/
```

Same financial fields as Contract Financials, aggregated at contract unit level.

### Cost Element Financials

```
GET /reporting/cost_elements/
```

Same financial fields, aggregated at cost element level.

### Project Financials

```
GET /reporting/projects/
```

Same financial fields, aggregated at project level. Also includes:
- `undistributed_budget`: Budget not yet allocated to cost elements

---

## Financial Field Structure

All monetary fields are objects with sub-fields:

```json
{
  "initial_budget": {
    "net": "395193.57",
    "gross": "470380.35"
  }
}
```

- `net`: Net amount (excluding tax)
- `gross`: Gross amount (including tax)

For this project, we use **net** values (Netto).

---

## Common Query Patterns

### Get all data for specific properties

```python
# 1. Get properties
properties = GET /properties/?filter[id.in]=uuid1,uuid2

# 2. Get projects for those properties
projects = GET /projects/?filter[property.in]=uuid1,uuid2

# 3. Get contract units for those projects
project_ids = [p.id for p in projects]
contract_units = GET /contract_units/?filter[project.in]=id1,id2,id3

# 4. Get contracts for those contract units
contract_unit_ids = [cu.id for cu in contract_units]
contracts = GET /contracts/?filter[contract_unit.in]=id1,id2,id3

# 5. Get financials
financials = GET /reporting/contracts/?filter[project.in]=id1,id2,id3
```

### Batch Fetching

For large datasets, fetch in batches to avoid timeouts:
- Batch size: ~50 IDs per request
- Add retry logic with exponential backoff

---

## Notes & Gotchas

1. **Contract numbers are not unique** - They are human-entered and can be duplicated or empty. Use `id` (UUID) for lookups.

2. **Empty contract units** - Most contract units have no contracts. Filter these out when building reports.

3. **Pagination required** - Large datasets are paginated. Always follow `links.next`.

4. **Rate limiting** - Be mindful of API rate limits. Add delays between batch requests.

5. **Financial fields can be null** - Always handle null values for monetary fields.

6. **Net vs Gross** - German accounting uses both. This project uses `net` (Netto) values.

7. **Filter format is strict** - Must use `filter[attribute.operation]` format (e.g., `filter[property.in]`), not `filter[attribute]`. Using wrong format returns 400 Bad Request.

---

## Real-World Observations (from testing)

Data volumes observed during implementation testing:

| Entity | Count | Notes |
|--------|-------|-------|
| Properties | 21 | Across entire Hines portfolio |
| Projects per Property | 1-5 | Varies by property |
| Contract Units per Project | ~540 | Many are empty templates |
| Contract Financials per Project | ~86 | Only units with actual contracts |

**Key insight**: Contract units vastly outnumber actual contracts because Alasco creates template structures. Always filter by financials to find active contract units.
