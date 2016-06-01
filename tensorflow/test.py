'''Write to a sql file'''

import sqlalchemy as sa

db = sa.create_engine('sqlite:///patent_database.db')
db.echo = False  # Try changing this to True and see what happens
metadata = sa.MetaData(db)
text = sa.Table('text2', metadata,
    sa.Column('patent_id', sa.String(), primary_key=True),
    sa.Column('abstract', sa.String()),
    sa.Column('description', sa.String()),
)
text.create()
i = text.insert()
i.execute(patent_id='Mary', abstract='12341235')

inventors = sa.Table('inventors', metadata,
    sa.Column('patent_id', sa.String(40), primary_key=True),
    sa.Column('index', sa.Integer),
    sa.Column('inventor', sa.string(40)),
)


inventors.select

s = text.select()
rs = s.execute()

row = rs.fetchone()
print 'Id:', row[0]
print 'Abstract:', row['abstract']
print 'abstract', row.abstract
print 'abstract', row[text.c.abstract]

for row in rs:
    print row.name, 'is', row.age, 'years old'