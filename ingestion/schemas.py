"""
Defines the event structure/schema.
"""

# Generic event schema for NexusFlow-X
EVENT_SCHEMA = {
	"event_id": "string",           # Unique identifier for each event
	"timestamp": "string",          # ISO format or Unix time
	"event_type": "string",         # Type of event (trip_start, delivery_update, etc.)
	"source": "string",             # Source of event (device, app, region)
	"status": "string",             # State (active, completed, error, etc.)
	"metrics": "dict",              # Numeric values (distance, temperature, etc.)
	"extra": "dict"                 # Additional info (optional)
}