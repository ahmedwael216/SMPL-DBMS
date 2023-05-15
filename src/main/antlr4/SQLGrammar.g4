grammar SQLGrammar;

statement: selectStatement | insertStatement | updateStatement | deleteStatement | createTableStatement | dropTableStatement | createIndexStatement;

selectStatement: SELECT columnList FROM tableName whereClause?;
insertStatement: INSERT INTO tableName LPAREN columnList RPAREN VALUES LPAREN valueList RPAREN;
updateStatement: UPDATE tableName SET setList whereClause?;
deleteStatement: DELETE FROM tableName whereClause?;
createTableStatement: CREATE TABLE tableName LPAREN columnDefinitions RPAREN;
dropTableStatement: DROP TABLE tableName;
createIndexStatement: CREATE INDEX indexName ON tableName LPAREN columnName RPAREN;

columnList: (columnName (COMMA columnName)*) | STAR;
valueList: (value (COMMA value)*);
setList: (columnName EQUALS value (COMMA columnName EQUALS value)*);
columnDefinitions: columnDefinition (COMMA columnDefinition)*;

expression: orexpression;
orexpression: andexpression (OR andexpression | XOR andexpression)*;
andexpression: notexpression (AND notexpression)*;
notexpression: NOT notexpression | comparison;
comparison: addexpression ((EQUALS | NOTEQUALS | LESSTHAN | GREATERTHAN | LESSTHANEQUAL | GREATERTHANEQUAL) addexpression)*;
addexpression: mulexpression ((PLUS | MINUS) mulexpression)*;
mulexpression: atom ((STAR | SLASH) atom)*;
atom: columnName | value | LPAREN expression RPAREN;

OR: 'OR';
AND: 'AND';
XOR: 'XOR';
NOT: 'NOT';
EQUALS: '=';
NOTEQUALS: '!=';
LESSTHAN: '<';
GREATERTHAN: '>';
LESSTHANEQUAL: '<=';
GREATERTHANEQUAL: '>=';

PLUS: '+';
MINUS: '-';
STAR: '*';
SLASH: '/';

whereClause: WHERE expression;

tableName: IDENTIFIER;
columnName: IDENTIFIER;
indexName: IDENTIFIER;

columnDefinition: columnName dataType (columnConstraint)*;
dataType: INT | CHAR LPAREN INT RPAREN;
columnConstraint: NOT NULL | PRIMARY KEY;

value: STRING | NUMBER | NULL;

LPAREN: '(';
RPAREN: ')';
COMMA: ',';

CREATE: 'CREATE';
DROP: 'DROP';
INDEX: 'INDEX';
TABLE: 'TABLE';
INT: 'INT';
CHAR: 'CHAR';
NULL: 'NULL';
PRIMARY: 'PRIMARY';
KEY: 'KEY';

IDENTIFIER: [a-zA-Z_][a-zA-Z0-9_]*;
STRING: '\'' ~('\'')* '\'';
NUMBER: [0-9]+('.'[0-9]+)?;

WS: [ \t\r\n]+ -> skip;