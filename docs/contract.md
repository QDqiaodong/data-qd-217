# Datagram fragment reassembly contract

Packet-capture data engineers provide fragments with globally unique identity, datagram identity, byte offset, a terminal flag and hexadecimal payload. Fragments may arrive out of order. Identical overlapping bytes are retained as corroborating coverage; conflicting overlapping bytes create explicit conflict evidence. Exactly one terminal length is required before completeness can be assessed.

A datagram is complete only when every byte from zero through the terminal end is covered and no byte extends beyond that end. Incomplete, conflicting, ambiguous-terminal and data-beyond-terminal states remain separate. The service does not parse IP headers, decrypt traffic, calculate transport checksums or contact a network.

The result is an offline byte-provenance data product, not a generic file tool or network monitoring dashboard. Future protocol decoders can consume only complete payloads while preserving fragment evidence.
