# importar librerías
import pandas as pd
from sqlalchemy import create_engine


db_config = {'user': 'practicum_student',         # nombre de usuario
             'pwd': 's65BlTKV3faNIGhmvJVzOqhs', # contraseña
             'host': 'rc1b-wcoijxj3yxfsf3fs.mdb.yandexcloud.net',
             'port': 6432,              # puerto de conexión
             'db': 'data-analyst-final-project-db'}          # nombre de la base de datos

connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                                                     db_config['pwd'],
                                                                       db_config['host'],
                                                                       db_config['port'],
                                                                       db_config['db'])

engine = create_engine(connection_string, connect_args={'sslmode':'require'})

query = 'SELECT * FROM books'
df = pd.io.sql.read_sql(query, con = engine)
print(df)


query = """SELECT count(*) as numero_libros FROM books 
WHERE publication_date >'2000-01-01' """
num_publica = pd.io.sql.read_sql(query,con=engine)
print('numero de libros publicados desde el primero de enero del 2000:',
      num_publica['numero_libros'][0])


query = """ SELECT 
ratings.book_id ,
AVG(ratings.rating) as promedio,
count(DISTINCT reviews.review_id) as numero_rese

FROM   
ratings INNER JOIN reviews ON reviews.book_id  = ratings.book_id 

group by 
ratings.book_id

order by
numero_rese desc;

"""

num_pro = pd.io.sql.read_sql(query,con= engine)
print()


query = """
SELECT
publishers.publisher,
books.publisher_id,
count(books.book_id) as nume_libros_editorial
FROM
books
INNER JOIN publishers on publishers.publisher_id  =books.publisher_id 
WHERE
books.num_pages > 50

group by
publishers.publisher,books.publisher_id

order by 
nume_libros_editorial desc
LIMIT 1;
"""

max_edito = pd.io.sql.read_sql(query ,con =  engine)
print(max_edito)


query = """ 
SELECT
    b.author_id,
    AVG(libros.promedio_libro) AS calificacion_promedio
FROM (
    SELECT
        ratings.book_id,
        AVG(ratings.rating) AS promedio_libro,
        COUNT(ratings.rating_id) AS num_ratings
    FROM ratings
    GROUP BY ratings.book_id
    HAVING COUNT(ratings.rating_id) >= 50
) AS libros
INNER JOIN books b ON b.book_id = libros.book_id
GROUP BY b.author_id
ORDER BY calificacion_promedio DESC
LIMIT 1;"""

m_a = pd.io.sql.read_sql(query,con= engine)
print(m_a)


query = """ 
SELECT

AVG(numero_rese) as promedio_calificacion


FROM  
(SELECT 
ratings.username,
COUNT(reviews.username) AS numero_rese

FROM ratings
INNER JOIN reviews ON reviews.username = ratings.username

GROUP BY
ratings.username
HAVING
COUNT(ratings.rating_id) > 50


) AS usarios_filt;

""" 

promedio_usarios = pd.io.sql.read_sql(query,con= engine)
print(promedio_usarios)