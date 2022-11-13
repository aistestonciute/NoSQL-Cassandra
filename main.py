from cassandra.cluster import Cluster
# from cassandra import ConsistencyLevel
from cassandra.query import tuple_factory
from cassandra.query import SimpleStatement
# from cassandra.query import dict_factory

KEYSPACE = "laboratorinis"

cluster = Cluster(port = 9042)
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """ % KEYSPACE)

session.set_keyspace(KEYSPACE)

def CreateTables():
    session.execute("""DROP TABLE IF EXISTS laboratorinis.author""")
    session.execute("""
           CREATE TABLE IF NOT EXISTS author (
               id int,
               firstName text,
               lastName text,
               birthYear int,
               origin text,
               PRIMARY KEY ((firstName, lastName), id)
           )
           """)

    session.execute("""DROP TABLE IF EXISTS laboratorinis.user""")
    session.execute("""
           CREATE TABLE IF NOT EXISTS user (
               id int,
               username text,
               email text,
               password text,
               PRIMARY KEY (id)
           )
           """)

    session.execute("""DROP TABLE IF EXISTS laboratorinis.book""")
    session.execute("""
           CREATE TABLE IF NOT EXISTS book (
               id int,
               author_id int,
               title text,
               year int,
               PRIMARY KEY (id, author_id)
           )
           """)

    session.execute("""DROP TABLE IF EXISTS laboratorinis.review""")
    session.execute("""
           CREATE TABLE IF NOT EXISTS review (
               id int,
               user_id int,
               book_id int,
               rating int,
               date text,
               text text,
               PRIMARY KEY (user_id, id)
           )
           """)

    session.execute("""DROP TABLE IF EXISTS laboratorinis.user_ue""")
    session.execute("""
           CREATE TABLE IF NOT EXISTS user_ue (
               id int,
               username text,
               email text,
               password text,
               PRIMARY KEY ((username, email))
           )
           """)


def InsertValues():
    query_a = SimpleStatement("""
            INSERT INTO author (id, firstName, lastName, birthYear, origin)
            VALUES (%(id)s, %(firstName)s,%(lastName)s, %(birthYear)s, %(origin)s)
            """)
    session.execute(query_a, dict(id = 1, firstName = 'Antanas', lastName = 'Skema', birthYear = 1910, origin = 'Polish/Lithuanian'))
    session.execute(query_a, dict(id = 2, firstName = 'Stephen', lastName = 'King', birthYear = 1947, origin = 'American'))
    session.execute(query_a, dict(id = 3, firstName = 'Kristijonas', lastName = 'Donelaitis', birthYear = 1714, origin = 'Lithuanian'))

    query_u = SimpleStatement("""
            INSERT INTO user (id, username, email, password)
            VALUES (%(id)s,%(username)s, %(email)s, %(password)s)
            """)
    session.execute(query_u, dict(id = 1, username = 'aiste.stonciute', email = 'aiste.stonciute@gmail.com', password = 'aiste123'))
    session.execute(query_u, dict(id = 2, username = 'vartotojas', email = 'vartotojas@gmail.com', password = 'slaptazodis456'))
    session.execute(query_u, dict(id = 3, username = 'knygius', email = 'knygius@info.com', password = 'knyga456789'))
    session.execute(query_u, dict(id = 4, username = 'aiste123', email = 'aiste@gmail.com', password = 'aiste123'))

    query_b = SimpleStatement("""
            INSERT INTO book (id, author_id, title, year)
            VALUES (%(id)s,%(author_id)s, %(title)s, %(year)s)
            """)
    session.execute(query_b, dict(id = 1, author_id = 1, title = 'Balta drobule', year = 1958))
    session.execute(query_b, dict(id = 2, author_id = 1, title = 'Zivile', year = 1948))
    session.execute(query_b, dict(id = 3, author_id = 3, title = 'Metai', year = 1765))
    session.execute(query_b, dict(id = 4, author_id = 2, title = 'IT', year = 1986))
    session.execute(query_b, dict(id = 5, author_id = 2, title = 'The Shining', year = 1977))
    session.execute(query_b, dict(id = 6, author_id = 2, title = 'Carrie', year = 1974))

    query_r = SimpleStatement("""
            INSERT INTO review (id, user_id, book_id, rating, date, text)
            VALUES (%(id)s,%(user_id)s, %(book_id)s, %(rating)s, %(date)s, %(text)s)
            """)
    session.execute(query_r, dict(id = 1, user_id = 2, book_id = 3, rating = 3, date = '2022-10-03', text = 'Boring'))
    session.execute(query_r, dict(id = 2, user_id = 2, book_id = 1, rating = 5, date = '2022-11-03', text = 'Amazing'))
    session.execute(query_r, dict(id = 3, user_id = 1, book_id = 3, rating = 3, date = '2022-10-01', text = 'Average book'))
    session.execute(query_r, dict(id = 4, user_id = 2, book_id = 4, rating = 1, date = '2022-10-12', text = 'Bad'))
    session.execute(query_r, dict(id = 5, user_id = 4, book_id = 2, rating = 4, date = '2022-10-13', text = 'Great book'))

    query_u_ue = SimpleStatement("""
            INSERT INTO user_ue (id, username, email, password)
            VALUES (%(id)s,%(username)s, %(email)s, %(password)s)
            """)
    session.execute(query_u_ue, dict(id = 1, username = 'aiste.stonciute', email = 'aiste.stonciute@gmail.com', password = 'aiste123'))
    session.execute(query_u_ue, dict(id = 2, username = 'vartotojas', email = 'vartotojas@gmail.com', password = 'slaptazodis456'))
    session.execute(query_u_ue, dict(id = 3, username = 'knygius', email = 'knygius@info.com', password = 'knyga456789'))
    session.execute(query_u_ue, dict(id = 4, username = 'aiste123', email = 'aiste@gmail.com', password = 'aiste123'))

def GetAuthorInfo():
    authors = session.execute("SELECT * FROM author")
    print("\nAuthors' information: ")
    for i in authors:
        author = i
        print("Author id: ", author.id, ", name: ", author.firstname, author.lastname, ", birth year: ", author.birthyear, ", origin: ", author.origin)

def GetUserInfo():
    users = session.execute("SELECT * FROM user")
    print("\nUsers' information: ")
    for i in users:
        user = i
        print("User id: ", user.id, ", username: ", user.username, ", email: ", user.email, ", password: ", user.password)

def GetBookInfo():
    books = session.execute("SELECT * FROM book")
    print("\nBooks' information: ")
    for i in books:
        book = i
        print("Book id: ", book.id, ", author id: ", book.author_id, ", title: ", book.title, ", year: ", book.year)

def GetReviewInfo():
    reviews = session.execute("SELECT * FROM review")
    print("\nReviews' information: ")
    for i in reviews:
        review = i
        print("Review id: ", review.id, ", user id: ", review.user_id, ", book id: ", review.book_id, ", rating: ", review.rating, ", date: ", review.date, ", text: ", review.text)

def GetUserInfoById(id):
    users = session.execute(f'SELECT * FROM user WHERE id = {id}')
    if not users:
        print('\nUser does not exist.')
    else:
        print('\nUser information by id:', id)
        for i in users:
            user = i
            print("username: ", user.username, ", email: ", user.email, ", password: ", user.password)
        print('Review written by this user: ')
        reviews = session.execute(f'SELECT * FROM review WHERE user_id = {id}')
        for j in reviews:
            review = j
            print("Review id: ", review.id, ", book id: ", review.book_id, ", rating: ", review.rating, ", date: ", review.date, ", text: ", review.text)

def GetUserInfoByUE(username, email):
    users = session.execute(f"SELECT * FROM user_ue WHERE username = '{username}' AND email = '{email}'")
    if not users:
        print('\nUser does not exist.')
    else:
        print('\nUser information by username and email:', username, ' ', email)
        for i in users:
            user = i
            print("user id: ", user.id, ", username: ", user.username, ", email: ", user.email, ", password: ", user.password)
            id = user.id
        print('Review written by this user: ')
        reviews = session.execute(f'SELECT * FROM review WHERE user_id = {id}')
        for j in reviews:
            review = j
            print("Review id: ", review.id, ", book id: ", review.book_id, ", rating: ", review.rating, ", date: ", review.date, ", text: ", review.text)

def InsertUser():
    id = input("Insert id: ")
    users = session.execute(f'SELECT * FROM user WHERE id = {id}')
    if not users:
        username = input("Insert username: ")
        email = input("Insert email: ")
        users2 = session.execute(f"SELECT * FROM user_ue WHERE username = '{username}' AND email = '{email}'")
        if not users2:
            password = input("Insert password: ")
            query_u_ue = SimpleStatement("""
                INSERT INTO user_ue (id, username, email, password)
                VALUES (%(id)s, %(username)s, %(email)s, %(password)s)
                IF NOT EXISTS
                """)
            session.execute(query_u_ue, dict(id= int (id), username = username, email = email, password = password))
            query_u = SimpleStatement("""
                INSERT INTO user (id, username, email, password)
                VALUES (%(id)s, %(username)s, %(email)s, %(password)s)
                IF NOT EXISTS
                """)
            session.execute(query_u, dict(id= int (id), username = username, email = email, password = password))
        else:
            print('\nUser already exists.')
            exit(0)
    else:
        print('\nUser already exists.')

def UpdateReview():
    id = input("\nInsert user id: ")
    users = session.execute(f'SELECT * FROM user WHERE id = {id}')
    if not users:
        print('\nUser does not exists.')
    else:
        for i in users:
            user = i
            id = user.id
        reviews = session.execute(f'SELECT * FROM review WHERE user_id = {id}')
        print('User reviews: ')
        for j in reviews:
            review = j
            print(review)

        review_id = input('Insert review id you want to update: ')
        rating = input('Input rating: ')
        text = input('Input review text: ')

        session.execute(f"UPDATE review SET book_id = {review.book_id}, rating = {rating}, date = '{review.date}', text = '{text}'" 
        f"WHERE id = {review_id} AND user_id = {id}")


def main():
    CreateTables()
    InsertValues()
    while True:
        print("""
    1 - Get information about authors
    2 - Get information about books
    3 - Get information about users
    4 - Get information about reviews
    5 - Get user by id
    6 - Get user by username and email
    7 - Insert new user
    8 - Update review
    9 - Exit
    """)
        action = input('Choose action: ')
        if(action == '1'):
            GetAuthorInfo()
        elif(action == '2'):
            GetBookInfo()
        elif(action == '3'):
            GetUserInfo()
        elif(action == '4'):
            GetReviewInfo()
        elif(action == '5'):
            id = input('Insert user id: ')
            GetUserInfoById(id)
        elif(action == '6'):
            username = input('Insert username: ')
            email = input('Insert email: ')
            GetUserInfoByUE(username, email)
        elif(action == '7'):
            InsertUser()
        elif(action == '8'):
            UpdateReview()
        elif(action == '9'):
            break
        else:
            print('Action does not exist')

    session.row_factory = tuple_factory
    session.execute("DROP KEYSPACE " + KEYSPACE)

if __name__ == "__main__":
    main()
