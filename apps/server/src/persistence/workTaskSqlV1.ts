// Freeze V1 once migrations consume it; SQL expressions are supplied by code, never callers.
const whitespace =
  "char(9,10,11,12,13,32,160,5760,8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,8232,8233,8239,8287,12288,65279)";

export const canonicalTextSql = (value: string) => `(typeof(${value}) IS 'text'
  AND length(CAST(${value} AS BLOB))>0 AND hex(${value}) IS hex(trim(${value},${whitespace}))
  AND NOT EXISTS (
    WITH RECURSIVE units(bytes,position,count) AS (
      SELECT hex(CAST(${value} AS BLOB)),1,0
      UNION ALL SELECT bytes,position+2,count+CASE
        WHEN substr(bytes,position,1) IN ('8','9','A','B') THEN 0
        WHEN substr(bytes,position,1)='F' THEN 2 ELSE 1 END
      FROM units WHERE position<=length(bytes) AND count<=2000
    ) SELECT 1 FROM units WHERE count>2000
  )) IS TRUE`;

const uniqueJsonSql = (
  value: string,
) => `(CASE WHEN typeof(${value}) IS 'text' AND json_valid(${value}) THEN NOT EXISTS (
  SELECT 1 FROM json_tree(${value}) WHERE key IS NOT NULL GROUP BY parent,key HAVING count(*)>1
) ELSE 0 END)`;

export const canonicalEvidenceJsonSql = (
  value: string,
) => `(CASE WHEN ${uniqueJsonSql(value)} THEN NOT EXISTS (
  SELECT 1 FROM json_tree(${value}) WHERE type='text' AND NOT (${canonicalTextSql("atom")})
) ELSE 0 END)`;

const jsonKindSql = (alias: string) =>
  `CASE WHEN ${alias}.type IN ('integer','real') THEN 'number' ELSE ${alias}.type END`;
const jsonAtomSql = (alias: string) =>
  `CASE WHEN ${alias}.type='text' THEN CAST(${alias}.atom AS BLOB) ELSE ${alias}.atom END`;

export const structuralJsonEqualSql = (left: string, right: string) => `(CASE
  WHEN NOT ${uniqueJsonSql(left)} OR NOT ${uniqueJsonSql(right)} THEN 0
  WHEN hex(${left}) IS hex(${right}) THEN 1
  ELSE (
    WITH RECURSIVE
    __task_left_v1 AS MATERIALIZED (SELECT id,parent,key,type,atom FROM json_tree(${left})),
    __task_right_v1 AS MATERIALIZED (SELECT id,parent,key,type,atom FROM json_tree(${right})),
    __task_pairs_v1(left_id,right_id) AS (
      SELECT a.id,b.id FROM __task_left_v1 a,__task_right_v1 b WHERE a.parent IS NULL AND b.parent IS NULL
      UNION ALL
      SELECT a.id,b.id FROM __task_pairs_v1 p
      JOIN __task_left_v1 a ON a.parent=p.left_id
      JOIN __task_right_v1 b ON b.parent=p.right_id
        AND typeof(a.key) IS typeof(b.key)
        AND (CASE WHEN typeof(a.key)='text' THEN CAST(a.key AS BLOB) ELSE a.key END)
          IS (CASE WHEN typeof(b.key)='text' THEN CAST(b.key AS BLOB) ELSE b.key END)
    )
    SELECT (SELECT count(*) FROM __task_pairs_v1)=(SELECT count(*) FROM __task_left_v1)
      AND (SELECT count(*) FROM __task_pairs_v1)=(SELECT count(*) FROM __task_right_v1)
      AND NOT EXISTS (
        SELECT 1 FROM __task_pairs_v1 p JOIN __task_left_v1 a ON a.id=p.left_id JOIN __task_right_v1 b ON b.id=p.right_id
        WHERE (${jsonKindSql("a")}) IS NOT (${jsonKindSql("b")}) OR (${jsonAtomSql("a")}) IS NOT (${jsonAtomSql("b")})
      )
  ) END)`;

export const validTimestampSql = (value: string) => {
  const zone = `(CASE WHEN substr(${value},-1)='Z' THEN 1 ELSE 6 END)`;
  const fraction = `substr(${value},20,length(${value})-19-${zone})`;
  return `(typeof(${value})='text' AND length(${value}) BETWEEN 20 AND 2000 AND length(${value})=length(CAST(${value} AS BLOB))
    AND substr(${value},1,4) NOT GLOB '*[^0-9]*' AND substr(${value},5,1)='-' AND substr(${value},8,1)='-'
    AND substr(${value},11,1)='T' AND substr(${value},14,1)=':' AND substr(${value},17,1)=':'
    AND substr(${value},6,2) NOT GLOB '*[^0-9]*' AND substr(${value},9,2) NOT GLOB '*[^0-9]*'
    AND substr(${value},12,2) NOT GLOB '*[^0-9]*' AND substr(${value},15,2) NOT GLOB '*[^0-9]*' AND substr(${value},18,2) NOT GLOB '*[^0-9]*'
    AND CAST(substr(${value},6,2) AS INTEGER) BETWEEN 1 AND 12
    AND CAST(substr(${value},9,2) AS INTEGER) BETWEEN 1 AND CASE CAST(substr(${value},6,2) AS INTEGER)
      WHEN 2 THEN CASE WHEN CAST(substr(${value},1,4) AS INTEGER)%4=0 AND (CAST(substr(${value},1,4) AS INTEGER)%100<>0 OR CAST(substr(${value},1,4) AS INTEGER)%400=0) THEN 29 ELSE 28 END
      WHEN 4 THEN 30 WHEN 6 THEN 30 WHEN 9 THEN 30 WHEN 11 THEN 30 ELSE 31 END
    AND CAST(substr(${value},12,2) AS INTEGER) BETWEEN 0 AND 23 AND CAST(substr(${value},15,2) AS INTEGER) BETWEEN 0 AND 59 AND CAST(substr(${value},18,2) AS INTEGER) BETWEEN 0 AND 59
    AND (substr(${value},-1)='Z' OR (length(${value})>=25 AND substr(${value},-6,1) IN ('+','-') AND substr(${value},-3,1)=':'
      AND substr(${value},-5,2) NOT GLOB '*[^0-9]*' AND substr(${value},-2,2) NOT GLOB '*[^0-9]*'
      AND CAST(substr(${value},-5,2) AS INTEGER) BETWEEN 0 AND 23 AND CAST(substr(${value},-2,2) AS INTEGER) BETWEEN 0 AND 59))
    AND (${fraction}='' OR (length(${fraction})>1 AND substr(${fraction},1,1)='.' AND substr(${fraction},2) NOT GLOB '*[^0-9]*'))) IS TRUE`;
};

export const timestampAtOrBeforeSql = (left: string, right: string) => `(CASE
  WHEN ${validTimestampSql(left)} AND ${validTimestampSql(right)} THEN (
    WITH __task_time_input_v1(side,value) AS (VALUES (0,(${left})),(1,(${right}))),
    __task_time_parts_v1 AS (
      SELECT side,CAST(substr(value,1,4) AS INTEGER) y,CAST(substr(value,6,2) AS INTEGER) m,CAST(substr(value,9,2) AS INTEGER) d,
        CAST(substr(value,12,2) AS INTEGER) h,CAST(substr(value,15,2) AS INTEGER) mi,CAST(substr(value,18,2) AS INTEGER) s,
        CASE WHEN substr(value,-1)='Z' THEN 0 ELSE (CASE substr(value,-6,1) WHEN '+' THEN 1 ELSE -1 END)
          *(CAST(substr(value,-5,2) AS INTEGER)*3600+CAST(substr(value,-2,2) AS INTEGER)*60) END offset_seconds,
        rtrim(CASE WHEN substr(value,20,1)='.' THEN substr(value,21,length(value)-20-(CASE WHEN substr(value,-1)='Z' THEN 1 ELSE 6 END)) ELSE '' END,'0') fraction
      FROM __task_time_input_v1
    ),
    __task_instants_v1 AS (
      SELECT side,fraction,
        (365*y+(y+3)/4-(y+99)/100+(y+399)/400-719528+d-1
          +CASE m WHEN 1 THEN 0 WHEN 2 THEN 31 WHEN 3 THEN 59 WHEN 4 THEN 90 WHEN 5 THEN 120 WHEN 6 THEN 151 WHEN 7 THEN 181 WHEN 8 THEN 212 WHEN 9 THEN 243 WHEN 10 THEN 273 WHEN 11 THEN 304 ELSE 334 END
          +CASE WHEN m>2 AND y%4=0 AND (y%100<>0 OR y%400=0) THEN 1 ELSE 0 END)*86400+h*3600+mi*60+s-offset_seconds AS whole_seconds
      FROM __task_time_parts_v1
    )
    SELECT a.whole_seconds<b.whole_seconds OR (a.whole_seconds=b.whole_seconds AND a.fraction<=b.fraction COLLATE BINARY)
    FROM __task_instants_v1 a,__task_instants_v1 b WHERE a.side=0 AND b.side=1
  ) ELSE 0 END)`;
