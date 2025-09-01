# Search Endpoints

## 1. Super Search
**GET** `/v1/search`

The all-in-one search endpoint for finding carriers and companies.

### Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `superSearchTerm` | string | No | Super search a string or phrase | "Trucking" |
| `docketNumber` | integer | No | Docket number (MC/MX/FF) | 596655 |
| `dotNumber` | integer | No | Department of Transportation Number | 1911857 |
| `companyTypes[]` | array | No | Filter by company types | ["Broker", "Carrier"] |
| `radiusZipcode` | string | No | Radius zipcode to filter by | "10001" |
| `radiusMiles` | integer | No | Radius miles to filter by | 10 |
| `addressState` | string | No | Address state of the company | "CA" |
| `minPowerUnits` | integer | No | Minimum number of power units | 1 |
| `maxPowerUnits` | integer | No | Maximum number of power units | 100 |
| `page` | integer | No | Page number (default: 1) | 1 |
| `perPage` | integer | No | Results per page (default: 10) | 10 |

### Company Types
- Broker
- Carrier
- Freight Forwarder
- Shipper
- Registrant
- Intermodal Equipment Provider
- Cargo Tank

### Response
Returns array of `Company v1` objects with standard pagination.

---

## 2. Search by SCAC
**GET** `/v1/search/scac`

Returns information related to a Standard Carrier Alpha Code (SCAC).

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `scac` | string | Yes | SCAC to search for |

### Response
Returns `Scac v1` object.

---

## 3. Search by VIN
**GET** `/v1/search/by-vin/{vin}`

Get all related companies for a given Vehicle Identification Number.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `vin` | string | Yes | Vehicle Identification Number (in path) |

### Response
Returns array of objects containing:
- `company`: Company object
- `related_date`: Date of relationship

### Use Cases
- Track vehicle ownership history
- Identify companies that have operated specific equipment
- Investigate equipment transfers between carriers