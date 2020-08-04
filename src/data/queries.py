import psycopg2
import config

con= None

# Hae kaikki person taulunrivit ja tulosta ne.
def search_rows(cursor):
    SQL = 'SELECT * FROM person;'
    cursor.execute(SQL)
    row = cursor.fetchall()
    print(row)
    cursor.close()

# Hae person taulun sarakkeiden nimet ja tulosta ne.
def search_columns(cursor):
    SQL = 'SELECT * FROM person;'
    cursor.execute(SQL)
    columns = cursor.description
    print(columns)

# Hae certificate taulun sarakkeiden nimet,sekä rivit ja tulosta ne.
def search_column_names(cursor):
    SQL = 'SELECT * FROM certificates;'
    cursor.execute(SQL)
    columns = cursor.description
    print(columns)
    row = cursor.fetchall()
    print(row)

# Hae kaikki AWS sertifikaattien omistajat.
def search_certificate_owners(cursor):
    SQL = "SELECT person.name FROM person" \
          " JOIN certificates ON person.id=certificates.person_id " \
          "WHERE certificates.name = 'SAA-CO2';"
    cursor.execute(SQL)
    columns = cursor.description
    print(columns)
    row = cursor.fetchall()
    print(row)

# Lisää uusi rivi certificate tauluun siten, että arvot otetaan function parametreinä.
def insert_certificate(cursor, con, name, person_id):
    SQL = "insert into certificates (name, person_id) values (%s, %s)"
    data = (name, person_id)
    cursor.execute(SQL, data)
    con.commit()

# Lisää uusi rivi person tauluun siten, että syötät values arvot jälkikäteen.
def insert_person(cursor, con, name, age, student):
    SQL = "insert into person (name, age, student) values (%s, %s, %s)"
    data = (name, age, student)
    cursor.execute(SQL, data)
    con.commit()

# Päivitä olemassa olevaa riviä person taulussa. Arvot otetaan function parametreinä.
def update_row_in_person(cursor, con, id, student):
    SQL = "UPDATE person SET student = %s WHERE id = %s"
    data = (id, student)
    cursor.execute(SQL, data)
    con.commit()

# Päivitä olemassa olevaa riviä certificate taulussa. Arvot otetaan function parametreinä.
def update_row_in_certificates(cursor, con, name, person_id):
    SQL = "UPDATE certificates SET name = %s WHERE person_id = %s"
    data = (name, person_id)
    cursor.execute(SQL, data)
    con.commit()

# Poista olemassa oleva rivi person taulusta. Poistettavan rivin tiedot otetaan function parametrinä.
def delete_row_in_person(cursor, con, id):
    SQL = "DELETE FROM person WHERE id = %s"
    data = (id)
    cursor.execute(SQL, data)
    con.commit()

# Poista olemassa oleva rivi certificate taulusta. Poistettavan rivin id otetaan function parametrinä.
def delete_row_in_certificates(cursor, con, person_id):
    SQL = "DELETE FROM certificates WHERE person_id = %s"
    data = (person_id)
    cursor.execute(SQL, data)
    con.commit()

# Tee funktio, joka luo uuden taulun tietokantaan. Voit päättää toteutuksen itse.
def create_table(cursor, con):
    SQL = """CREATE TABLE work_experience
                (id SERIAL PRIMARY KEY, 
                title VARCHAR(255) NOT NULL, 
                employer VARCHAR(255) NOT NULL,
                person_id int,
                CONSTRAINT fk_person FOREIGN KEY(person_id)
                REFERENCES person(id))"""
    data = ()
    cursor.execute(SQL, data)
    con.commit()

def connect():
    try:
        con = psycopg2.connect(**config.config())
        cursor = con.cursor()
        # search_rows(cursor)
        # search_columns(cursor)
        # search_column_names(cursor)
        # search_certificate_owners(cursor)
        # insert_certificate(cursor, con, 'PSM-3', 3)
        # insert_person(cursor, con, 'Milla', 27, True)
        # update_row_in_person(cursor, con, True, 7)
        # update_row_in_certificates(cursor, con, 'PSM-2', 8)
        # delete_row_in_person(cursor, con, '9')
        # delete_row_in_certificates(cursor, con, '2')
        # create_table(cursor, con)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

if __name__ == '__main__':
    connect()




