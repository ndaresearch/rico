# SearchCarriers API Documentation

## Overview

**Base URL**: `https://searchcarriers.com/api`

**Version**: 0.0.1

## Description

Welcome to the SearchCarriers API! This API provides comprehensive data about trucking carriers, including:
- Company information and search capabilities
- Insurance details and history
- Safety summaries and violations
- Authority status and history
- Equipment and vehicle information
- Inspection records
- Out of service orders

## Authentication

The API uses Bearer token authentication. To get started:

1. Go to your profile settings
2. Navigate to the API Tokens section at https://searchcarriers.com/settings/api-tokens
3. Create a new token with appropriate permissions
4. Include the token in your requests using the `Authorization: Bearer <token>` header

**Important**: Keep your token secure and never share it publicly!

## Rate Limits

Please check with the API provider for current rate limits and usage guidelines.

## Available Endpoints

The API is organized into the following main categories:

1. **Search** - Company search and lookup
2. **Authority** - Authority history and status
3. **Company Details** - Detailed company information
4. **Equipment** - Vehicle and equipment data
5. **Inspection** - Inspection records and details
6. **Vetting** - Risk assessment and safety ratings
7. **Export** - Data export capabilities
8. **Company Watches** - Monitoring and alerts

## Response Format

All endpoints return JSON responses with standard pagination structure:

```json
{
  "data": [...],
  "links": {
    "first": "...",
    "last": "...",
    "prev": "...",
    "next": "..."
  },
  "meta": {
    "current_page": 1,
    "from": 1,
    "last_page": 10,
    "path": "...",
    "per_page": 10,
    "to": 10,
    "total": 100
  }
}
```