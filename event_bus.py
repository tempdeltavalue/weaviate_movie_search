# event_bus.py
from collections import defaultdict
import sys
class EventBus:
    def __init__(self):
        self._handlers = defaultdict(list)

    def subscribe(self, event_name, handler):
        """Register a function to be called when an event is published."""
        self._handlers[event_name].append(handler)
        print(f"Subscribed handler '{handler.__name__}' to event '{event_name}'.")

    def publish(self, event_name, **kwargs):
        """Publish an event, calling all subscribed handlers."""
        print(f"\nPublishing event '{event_name}'...")
        if event_name in self._handlers:
            for handler in self._handlers[event_name]:
                try:
                    handler(**kwargs)
                except TypeError as e:
                    print(f"Error calling handler '{handler.__name__}': {e}", file=sys.stderr)

event_bus = EventBus()