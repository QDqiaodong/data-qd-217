def process(data):
    fragments=data.get("fragments") if isinstance(data,dict) else None
    if not isinstance(fragments,list) or not 1 <= len(fragments) <= 10000: raise ValueError("fragments must contain 1..10000 items")
    groups={}; ids=set()
    for row in fragments:
        fid=row.get("id") if isinstance(row,dict) else None
        key=row.get("datagram_id") if isinstance(row,dict) else None
        offset=row.get("offset") if isinstance(row,dict) else None
        more=row.get("more") if isinstance(row,dict) else None
        payload=row.get("payload_hex") if isinstance(row,dict) else None
        if not isinstance(fid,str) or not fid or fid in ids: raise ValueError("fragment ids must be unique")
        if not isinstance(key,str) or not key: raise ValueError("datagram_id must be nonblank")
        if isinstance(offset,bool) or not isinstance(offset,int) or offset < 0: raise ValueError("offset must be nonnegative integer")
        if not isinstance(more,bool): raise ValueError("more must be boolean")
        if not isinstance(payload,str) or len(payload)%2 or len(payload)>131070:
            raise ValueError("payload_hex must contain bounded complete bytes")
        try: raw=bytes.fromhex(payload)
        except ValueError as exc: raise ValueError("invalid payload_hex") from exc
        if not raw: raise ValueError("empty fragments are unsupported")
        ids.add(fid); groups.setdefault(key,[]).append((fid,offset,more,raw))
    results=[]
    for key,rows in sorted(groups.items()):
        terminal_ends={offset+len(raw) for _,offset,more,raw in rows if not more}
        conflicts=[]; byte_map={}; sources={}
        for fid,offset,_,raw in sorted(rows,key=lambda x:(x[1],x[0])):
            for index,value in enumerate(raw,offset):
                if index in byte_map and byte_map[index] != value:
                    conflicts.append({"offset":index,"fragment_ids":sorted([sources[index],fid])})
                else:
                    byte_map[index]=value; sources.setdefault(index,fid)
        if conflicts:
            status="conflict"; payload=None; missing=[]
        elif len(terminal_ends) != 1:
            status="missing_or_ambiguous_terminal"; payload=None; missing=[]
        else:
            end=next(iter(terminal_ends)); missing=[i for i in range(end) if i not in byte_map]
            if any(i >= end for i in byte_map):
                status="data_beyond_terminal"; payload=None
            elif missing:
                status="incomplete"; payload=None
            else:
                status="complete"; payload=bytes(byte_map[i] for i in range(end)).hex()
        results.append({"datagram_id":key,"status":status,"payload_hex":payload,"missing_offsets":missing,"conflicts":conflicts,"fragment_ids":sorted(x[0] for x in rows)})
    return {"datagrams":results}
