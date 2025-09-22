import pytds
conn = pytds.connect('10.148.144.71', 'yntti', 'sa', '123456', port=1433, use_mars=True)
cur = conn.cursor(); cur.execute('select 1'); print(cur.fetchone())