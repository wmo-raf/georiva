"""Per-user identity: API keys and the account surfaces that manage them.

Deliberately not part of ``organisations``. What lives here belongs to a
*person* and travels with them across every organisation they are a member of;
what lives there belongs to an institution. Keeping the two apart is what stops
a key from growing an organisation field.
"""
