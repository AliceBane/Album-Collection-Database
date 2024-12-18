import psycopg2

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="AlbumCollection",
            user="postgres",
            password="CoolPassword123",
            host="localhost"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        exit()

def create_artist(conn):
    name = input("Enter artist name: ")
    if not name:
        print("Artist name cannot be empty.")
        return

    with conn.cursor() as cur:
        cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (name,))
        artist = cur.fetchone()
        if artist:
            print("Artist already exists.")
            return

        # Create the artist
        cur.execute("INSERT INTO Artists (Name) VALUES (%s) RETURNING ArtistID;", (name,))
        artist_id = cur.fetchone()[0]

        # Prompt for songs
        print("Existing songs:")
        list_songs(conn)
        song_titles = input("Enter song titles separated by commas for the new artist: ").strip()
        if not song_titles or not any(song.strip() for song in song_titles.split(',')):
            print("An artist must have at least one song. Operation canceled.")
            conn.rollback()
            return

        for song_title in song_titles.split(','):
            song_title = song_title.strip()
            if not song_title:
                print("Song title cannot be empty. Operation canceled.")
                conn.rollback()
                return

            # Prompt for albums
            print("Existing albums:")
            list_albums(conn)
            album_titles = input(f"Enter album titles separated by commas for the song '{song_title}': ").strip()
            if not album_titles or not any(album.strip() for album in album_titles.split(',')):
                print("Each song must belong to at least one album. Operation canceled.")
                conn.rollback()
                return

            # Prompt for categories
            print("Existing categories:")
            list_categories(conn)
            categories = input(f"Enter category names separated by commas for the song '{song_title}': ").strip()
            if not categories or not any(category.strip() for category in categories.split(',')):
                print("Each song must have at least one category. Operation canceled.")
                conn.rollback()
                return

            # Process each album mentioned for the song
            for album_title in album_titles.split(','):
                album_title = album_title.strip()
                if not album_title:
                    print("Album title cannot be empty. Operation canceled.")
                    conn.rollback()
                    return

                # Validate and insert album
                cur.execute("SELECT AlbumID FROM Albums WHERE Title = %s;", (album_title,))
                album = cur.fetchone()

                if not album:
                    while True:
                        year = input(f"Enter the year the album '{album_title}' was released: ").strip()
                        if not year.isdigit():
                            print("Invalid year. Please enter a numeric year.")
                        else:
                            year = int(year)
                            break
                    # Create the album and link it to the artist
                    cur.execute("INSERT INTO Albums (Title, Year, ArtistID) VALUES (%s, %s, %s) RETURNING AlbumID;", (album_title, year, artist_id))
                    album_id = cur.fetchone()[0]
                else:
                    album_id = album[0]

                # Validate and insert song
                cur.execute("SELECT SongID FROM Songs WHERE Title = %s;", (song_title,))
                song = cur.fetchone()
                if not song:
                    cur.execute("INSERT INTO Songs (Title) VALUES (%s) RETURNING SongID;", (song_title,))
                    song_id = cur.fetchone()[0]
                else:
                    song_id = song[0]

                # Handle links
                cur.execute("SELECT * FROM SongArtists WHERE SongID = %s AND ArtistID = %s;", (song_id, artist_id))
                if not cur.fetchone():
                    cur.execute("INSERT INTO SongArtists (SongID, ArtistID) VALUES (%s, %s);", (song_id, artist_id))
                cur.execute("SELECT 1 FROM SongAlbums WHERE SongID = %s AND AlbumID = %s;", (song_id, album_id))
                if not cur.fetchone():
                    cur.execute("INSERT INTO SongAlbums (SongID, AlbumID) VALUES (%s, %s);", (song_id, album_id))
                cur.execute("SELECT * FROM AlbumArtists WHERE AlbumID = %s AND ArtistID = %s;", (album_id, artist_id))
                if not cur.fetchone():
                    cur.execute("INSERT INTO AlbumArtists (AlbumID, ArtistID) VALUES (%s, %s);", (album_id, artist_id))

                # Process categories
                for category in categories:
                    category = category.strip()
                    if not category:
                        print("Category name cannot be empty. Operation canceled.")
                        conn.rollback()
                        return
                    cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (category,))
                    category_record = cur.fetchone()
                    if not category_record:
                        cur.execute("INSERT INTO Categories (Name) VALUES (%s) RETURNING CategoryID;", (category,))
                        category_id = cur.fetchone()[0]
                    else:
                        category_id = category_record[0]
                    cur.execute("SELECT * FROM SongCategories WHERE SongID = %s AND CategoryID = %s;",
                                (song_id, category_id))
                    if not cur.fetchone():
                        cur.execute("INSERT INTO SongCategories (SongID, CategoryID) VALUES (%s, %s);", (song_id, category_id))

        conn.commit()

def create_album(conn):
    # Input album title and validate
    while True:
        title = input("Enter album title: ").strip()
        if title:
            break
        print("Album title cannot be empty. Please try again.")

    # Input and validate the year
    while True:
        try:
            year = int(input("Enter the year the album was released: ").strip())
            break
        except ValueError:
            print("Invalid input. Please enter a valid year (integer).")

    # Input artist names and validate
    while True:
        print("Existing artists:")
        list_artists(conn)
        artist_names = input("Enter the artist names separated by commas for the album: ").strip()
        if artist_names:
            artist_names = [name.strip() for name in artist_names.split(',') if name.strip()]
            if artist_names:
                break
        print("You must enter at least one artist name. Please try again.")

    # Input song titles and validate
    while True:
        print("Existing songs:")
        list_songs(conn)
        song_titles = input("Enter song titles separated by commas for this album: ").strip()
        if song_titles:
            song_titles = [title.strip() for title in song_titles.split(',') if title.strip()]
            # Check for duplicate song titles
            if len(set(song_titles)) != len(song_titles):
                print("Duplicate song titles found. Please provide unique song titles.")
                continue
            if song_titles:
                break
        print("You must enter at least one song title. Please try again.")

    with conn.cursor() as cur:
        try:
            # Check if the album already exists
            cur.execute("SELECT AlbumID FROM Albums WHERE Title = %s AND Year = %s;", (title, year))
            album = cur.fetchone()
            if album:
                print(f"Album '{title}' from year {year} already exists. No duplicates are allowed.")
                return

            # Create the album if it does not exist
            cur.execute("INSERT INTO Albums (Title, Year) VALUES (%s, %s) RETURNING AlbumID;", (title, year))
            album_id = cur.fetchone()[0]

            # Process each artist linked to this album
            for artist_name in artist_names:
                cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (artist_name,))
                artist = cur.fetchone()
                if not artist:
                    # Create the artist if they do not exist
                    cur.execute("INSERT INTO Artists (Name) VALUES (%s) RETURNING ArtistID;", (artist_name,))
                    artist_id = cur.fetchone()[0]
                else:
                    artist_id = artist[0]

                # Link the album with the artist
                cur.execute("INSERT INTO AlbumArtists (AlbumID, ArtistID) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (album_id, artist_id))

            # Process each song linked to this album
            for song_title in song_titles:
                print("Existing categories:")
                list_categories(conn)
                category_names = input(f"Enter category names separated by commas for the song '{song_title}': ").strip()
                category_names = [name.strip() for name in category_names.split(',') if name.strip()]

                # Check if the song already exists and link or create it
                cur.execute("SELECT SongID FROM Songs WHERE Title = %s;", (song_title,))
                song = cur.fetchone()
                if not song:
                    cur.execute("INSERT INTO Songs (Title) VALUES (%s) RETURNING SongID;", (song_title,))
                    song_id = cur.fetchone()[0]
                else:
                    song_id = song[0]

                # Link the song with the album
                cur.execute("INSERT INTO SongAlbums (SongID, AlbumID) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (song_id, album_id))

                # Link the song with each artist mentioned for the album
                for artist_name in artist_names:
                    cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (artist_name,))
                    artist_id = cur.fetchone()[0]
                    cur.execute("INSERT INTO SongArtists (SongID, ArtistID) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (song_id, artist_id))

                # Handle and link categories to the song
                for category_name in category_names:
                    cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (category_name,))
                    category = cur.fetchone()
                    if not category:
                        cur.execute("INSERT INTO Categories (Name) VALUES (%s) RETURNING CategoryID;", (category_name,))
                        category_id = cur.fetchone()[0]
                    else:
                        category_id = category[0]

                    # Link the song with the category
                    cur.execute("INSERT INTO SongCategories (SongID, CategoryID) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (song_id, category_id))

            conn.commit()
            print(f"Album '{title}' linked with artists: {', '.join(artist_names)} and songs: {', '.join(song_titles)}.")

        except Exception as e:
            conn.rollback()
            print(f"An error occurred while creating the album: {e}")
def create_category(conn):
    name = input("Enter category name: ").strip()
    if not name:
        print("Category name cannot be empty. Operation canceled.")
        return

    with conn.cursor() as cur:
        cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (name,))
        category = cur.fetchone()
        if category:
            print("Category already exists. Operation canceled.")
            return

        try:
            # Create the category
            cur.execute("INSERT INTO Categories (Name) VALUES (%s) RETURNING CategoryID;", (name,))
            category_id = cur.fetchone()[0]
            conn.commit()
            print(f"Category '{name}' created successfully.")
        except Exception as e:
            conn.rollback()
            print(f"An error occurred: {e}. Operation canceled.")

def create_song(conn):
    title = input("Enter song title: ")
    if not title:
        print("Song title cannot be empty. Operation canceled.")
        return

    with conn.cursor() as cur:
        cur.execute("SELECT SongID FROM Songs WHERE Title = %s;", (title,))
        song = cur.fetchone()
        if song:
            print("Song already exists. Operation canceled.")
            return

        try:
            print("Existing artists:")
            list_artists(conn)
            artist_names = input("Enter the artist names separated by commas for the song: ").strip()
            if not artist_names or not any(artist.strip() for artist in artist_names.split(',')):
                print("Each song must have at least one artist. Operation canceled.")
                conn.rollback()
                return

            print("Existing albums:")
            list_albums(conn)
            album_titles = input("Enter the album titles separated by commas this song belongs to: ").strip()
            if not album_titles or not any(album.strip() for album in album_titles.split(',')):
                print("Each song must belong to at least one album. Operation canceled.")
                conn.rollback()
                return

            print("Existing categories:")
            list_categories(conn)
            category_names = input("Enter category names separated by commas for the song: ").strip()
            if not category_names or not any(category.strip() for category in category_names.split(',')):
                print("Each song must have at least one category. Operation canceled.")
                conn.rollback()
                return

            # Create the song
            cur.execute("INSERT INTO Songs (Title) VALUES (%s) RETURNING SongID;", (title,))
            song_id = cur.fetchone()[0]

            # Link artists
            for artist_name in artist_names.split(','):
                artist_name = artist_name.strip()
                if not artist_name:
                    print("Artist name cannot be empty. Operation canceled.")
                    conn.rollback()
                    return

                cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (artist_name,))
                artist = cur.fetchone()
                if not artist:
                    cur.execute("INSERT INTO Artists (Name) VALUES (%s) RETURNING ArtistID;", (artist_name,))
                    artist_id = cur.fetchone()[0]
                else:
                    artist_id = artist[0]

                cur.execute("INSERT INTO SongArtists (SongID, ArtistID) VALUES (%s, %s);", (song_id, artist_id))

            # Link albums
            for album_title in album_titles.split(','):
                album_title = album_title.strip()
                if not album_title:
                    print("Album title cannot be empty. Operation canceled.")
                    conn.rollback()
                    return

                cur.execute("SELECT AlbumID FROM Albums WHERE Title = %s;", (album_title,))
                album = cur.fetchone()
                if not album:
                    while True:
                        year = input(f"Enter the year the album '{album_title}' was released: ").strip()
                        if not year.isdigit():
                            print("Invalid year. Please enter a numeric year.")
                        else:
                            year = int(year)
                            break
                    cur.execute("INSERT INTO Albums (Title, Year) VALUES (%s, %s) RETURNING AlbumID;", (album_title, year))
                    album_id = cur.fetchone()[0]
                    cur.execute("INSERT INTO AlbumArtists (AlbumID, ArtistID) VALUES (%s, %s);", (album_id, artist_id))
                else:
                    album_id = album[0]

                cur.execute("INSERT INTO SongAlbums (SongID, AlbumID) VALUES (%s, %s);", (song_id, album_id))

            # Link categories
            for category_name in category_names.split(','):
                category_name = category_name.strip()
                if not category_name:
                    print("Category name cannot be empty. Operation canceled.")
                    conn.rollback()
                    return

                cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (category_name,))
                category = cur.fetchone()
                if not category:
                    cur.execute("INSERT INTO Categories (Name) VALUES (%s) RETURNING CategoryID;",
                                (category_name,))
                    category_id = cur.fetchone()[0]
                else:
                    category_id = category[0]

                cur.execute("INSERT INTO SongCategories (SongID, CategoryID) VALUES (%s, %s);",
                            (song_id, category_id))

            # Commit the transaction
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"An error occurred: {e}. Operation canceled.")

def list_artists(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT a.Name, array_agg(al.Title) AS Albums
        FROM Artists a
        LEFT JOIN AlbumArtists aa ON a.ArtistID = aa.ArtistID
        LEFT JOIN Albums al ON aa.AlbumID = al.AlbumID
        GROUP BY a.ArtistID
        ORDER BY a.Name;
        """)
        artists = cur.fetchall()
        if not artists:
            print("No artists found.")
        else:
            print("Artists and their albums:")
            for name, albums in artists:
                album_list = ', '.join(filter(None, albums)) if albums[0] is not None else "No albums"
                print(f"Artist: {name}, Albums: {album_list}")

def list_albums(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT al.Title, al.Year, array_agg(DISTINCT a.Name) AS Artists, array_agg(DISTINCT s.Title) AS Songs
        FROM Albums al
        LEFT JOIN AlbumArtists aa ON al.AlbumID = aa.AlbumID
        LEFT JOIN Artists a ON aa.ArtistID = a.ArtistID
        LEFT JOIN SongAlbums sa ON al.AlbumID = sa.AlbumID
        LEFT JOIN Songs s ON sa.SongID = s.SongID
        GROUP BY al.AlbumID
        ORDER BY al.Title;
        """)
        albums = cur.fetchall()
        if not albums:
            print("No albums found.")
        else:
            print("Albums and their details:")
            for title, year, artists, songs in albums:
                artist_list = ', '.join(filter(None, artists)) if artists[0] is not None else "No artists"
                song_list = ', '.join(filter(None, songs)) if songs[0] is not None else "No songs"
                print(f"Album: {title}, Year: {year}, Artists: {artist_list}, Songs: {song_list}")

def list_categories(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT c.Name, array_agg(s.Title) AS Songs
        FROM Categories c
        LEFT JOIN SongCategories sc ON c.CategoryID = sc.CategoryID
        LEFT JOIN Songs s ON sc.SongID = s.SongID
        GROUP BY c.CategoryID
        ORDER BY c.Name;
        """)
        categories = cur.fetchall()
        if not categories:
            print("No categories found.")
        else:
            print("Categories and their songs:")
            for name, songs in categories:
                if songs[0] is not None:
                    print(f"Category: {name}, Songs: {', '.join(songs)}")
                else:
                    print(f"Category: {name}, Songs: None")

def list_songs(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT s.Title, array_agg(DISTINCT al.Title) AS Albums, array_agg(DISTINCT art.Name) AS Artists, array_agg(DISTINCT c.Name) AS Categories
        FROM Songs s
        LEFT JOIN SongAlbums sa ON s.SongID = sa.SongID
        LEFT JOIN Albums al ON sa.AlbumID = al.AlbumID
        LEFT JOIN SongArtists saa ON s.SongID = saa.SongID
        LEFT JOIN Artists art ON saa.ArtistID = art.ArtistID
        LEFT JOIN SongCategories sc ON s.SongID = sc.SongID
        LEFT JOIN Categories c ON sc.CategoryID = c.CategoryID
        GROUP BY s.SongID
        ORDER BY s.Title;
        """)
        songs = cur.fetchall()
        if not songs:
            print("No songs found.")
        else:
            print("Songs and their details:")
            for title, albums, artists, categories in songs:
                album_list = ', '.join(filter(None, set(albums)))  # Remove None and duplicates
                artist_list = ', '.join(filter(None, set(artists)))  # Remove None and duplicates
                category_list = ', '.join(filter(None, set(categories)))  # Remove None and duplicates
                print(f"Song: {title}, Albums: {album_list}, Artists: {artist_list}, Categories: {category_list}")

def delete_artist(conn):
    with conn.cursor() as cur:
        # List existing artists
        list_artists(conn)
        artist_name = input("Enter the name of the artist to delete: ")

        # Check if the artist exists
        cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (artist_name,))
        artist = cur.fetchone()
        if not artist:
            print("Artist not found.")
            return

        artist_id = artist[0]

        print(f"WARNING: Deleting artist '{artist_name}' will also delete their exclusive albums and songs.")
        confirm = input("Do you want to proceed? (yes/no): ")
        if confirm.lower() != "yes":
            print("Deletion canceled.")
            return

        try:
            # Disable foreign key constraints
            cur.execute("SET session_replication_role = 'replica';")

            # Identify and delete albums exclusively linked to this artist
            cur.execute("""
                SELECT al.AlbumID
                FROM Albums al
                JOIN AlbumArtists aa ON al.AlbumID = aa.AlbumID
                WHERE aa.ArtistID = %s
                AND NOT EXISTS (
                    SELECT 1 FROM AlbumArtists aa2
                    WHERE aa2.AlbumID = al.AlbumID AND aa2.ArtistID != %s
                );
            """, (artist_id, artist_id))
            albums = cur.fetchall()

            for album in albums:
                album_id = album[0]

                # Identify and delete songs exclusively linked to this album
                cur.execute("""
                    SELECT s.SongID
                    FROM Songs s
                    JOIN SongAlbums sa ON s.SongID = sa.SongID
                    WHERE sa.AlbumID = %s
                    AND NOT EXISTS (
                        SELECT 1 FROM SongAlbums sa2
                        WHERE sa2.SongID = s.SongID AND sa2.AlbumID != %s
                    );
                """, (album_id, album_id))
                songs = cur.fetchall()

                for song in songs:
                    song_id = song[0]
                    # Delete references to the song in junction tables
                    cur.execute("DELETE FROM SongArtists WHERE SongID = %s;", (song_id,))
                    cur.execute("DELETE FROM SongCategories WHERE SongID = %s;", (song_id,))
                    cur.execute("DELETE FROM SongAlbums WHERE SongID = %s;", (song_id,))
                    cur.execute("DELETE FROM Songs WHERE SongID = %s;", (song_id,))

                # Delete references to the album in junction tables
                cur.execute("DELETE FROM AlbumArtists WHERE AlbumID = %s;", (album_id,))
                cur.execute("DELETE FROM SongAlbums WHERE AlbumID = %s;", (album_id,))
                cur.execute("DELETE FROM Albums WHERE AlbumID = %s;", (album_id,))

            # Delete references to the artist in junction tables
            cur.execute("DELETE FROM AlbumArtists WHERE ArtistID = %s;", (artist_id,))
            cur.execute("DELETE FROM SongArtists WHERE ArtistID = %s;", (artist_id,))

            # Delete the artist
            cur.execute("DELETE FROM Artists WHERE ArtistID = %s;", (artist_id,))

            # Commit the transaction
            conn.commit()
            print(f"Artist '{artist_name}' and all associated data deleted successfully.")

        except Exception as e:
            conn.rollback()
            print(f"An error occurred: {e}")
        finally:
            # Re-enable foreign key constraints
            cur.execute("SET session_replication_role = 'origin';")

def delete_album(conn):
    cur = conn.cursor()
    print("Existing albums:")
    cur.execute("SELECT AlbumID, Title FROM Albums;")
    for album in cur.fetchall():
        print(f"- {album[1]}")

    album_title = input("Enter the title of the album to delete: ")
    cur.execute("SELECT AlbumID FROM Albums WHERE Title = %s;", (album_title,))
    album = cur.fetchone()

    if not album:
        print("Album not found.")
        return

    print("WARNING: Deleting album '{album_title}' will also delete its exclusive artists and songs.")
    confirm = input("Do you want to proceed? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Deletion canceled.")
        return

    cur.execute("BEGIN;")

    try:
        album_id = album[0]

        # Identify songs that are only linked to this album
        cur.execute("""
            SELECT SongID FROM SongAlbums WHERE AlbumID = %s
        """, (album[0],))
        song_ids_for_deletion = [song[0] for song in cur.fetchall()]

        # Remove these songs from the SongAlbums table
        cur.execute("DELETE FROM SongAlbums WHERE AlbumID = %s;", (album[0],))

        # For each song only linked to this deleted album, clear out entries from related tables
        for song_id in song_ids_for_deletion:
            cur.execute("SELECT COUNT(*) FROM SongAlbums WHERE SongID = %s;", (song_id,))
            if cur.fetchone()[0] == 0:
                cur.execute("DELETE FROM SongArtists WHERE SongID = %s;", (song_id,))
                cur.execute("DELETE FROM SongCategories WHERE SongID = %s;", (song_id,))
                cur.execute("DELETE FROM Songs WHERE SongID = %s;", (song_id,))

                # Check for artists exclusively linked to this album and its songs
            cur.execute("""
                   SELECT a.ArtistID
                   FROM Artists a
                   JOIN AlbumArtists aa ON a.ArtistID = aa.ArtistID
                   WHERE aa.AlbumID = %s
                   AND NOT EXISTS (
                       SELECT 1 FROM AlbumArtists aa2
                       WHERE aa2.ArtistID = a.ArtistID
                       AND aa2.AlbumID != %s
                   )
               """, (album_id, album_id))
            artist_ids_for_deletion = [artist[0] for artist in cur.fetchall()]

            # Delete these artists from junction tables and the main table
            for artist_id in artist_ids_for_deletion:
                cur.execute("DELETE FROM SongArtists WHERE ArtistID = %s;", (artist_id,))
                cur.execute("DELETE FROM AlbumArtists WHERE ArtistID = %s;", (artist_id,))
                cur.execute("DELETE FROM Artists WHERE ArtistID = %s;", (artist_id,))

        # Delete the album
        cur.execute("DELETE FROM Albums WHERE AlbumID = %s;", (album[0],))

        # Commit the transaction
        cur.execute("COMMIT;")
        print("Album and any exclusive songs and artists successfully deleted.")
    except Exception as e:
        print(f"An error occurred: {e}")
        cur.execute("ROLLBACK;")
    finally:
        cur.execute("SET session_replication_role = 'origin';")

def delete_category(conn):
    cur = conn.cursor()
    try:
        cur.execute("SET session_replication_role = replica;")

        # Display existing categories
        cur.execute("SELECT CategoryID, Name FROM Categories;")
        categories = cur.fetchall()
        if not categories:
            print("No categories found.")
            return

        print("Existing categories:")
        for cat in categories:
            print(f"- {cat[1]}")

        category_name = input("Enter the name of the category to delete: ")
        cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (category_name,))
        category = cur.fetchone()

        if not category:
            print("Category not found.")
            return

        print(f"WARNING: Deleting category '{category_name}' will also delete songs, albums, and artists related to it.")
        confirm = input("Do you want to proceed? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Deletion canceled.")
            return

        category_id = category[0]

        # Check songs that only belong to this category
        cur.execute("""
            SELECT s.SongID
            FROM Songs s
            JOIN SongCategories sc ON s.SongID = sc.SongID
            WHERE sc.CategoryID = %s
            AND NOT EXISTS (
                SELECT 1 FROM SongCategories sc2
                WHERE sc2.SongID = s.SongID AND sc2.CategoryID != %s
            );
        """, (category_id, category_id))
        songs = cur.fetchall()
        song_ids = [song[0] for song in songs]

        if song_ids:
            for song_id in song_ids:
                cur.execute("SELECT Title FROM Songs WHERE SongID = %s;", (song_id,))
                song_title = cur.fetchone()[0]
                print(f"- {song_title}")

            # Delete all references to these songs
            for song_id in song_ids:
                # Remove references to the song from other junction tables
                cur.execute("DELETE FROM SongAlbums WHERE SongID = %s;", (song_id,))
                cur.execute("DELETE FROM SongArtists WHERE SongID = %s;", (song_id,))
                cur.execute("DELETE FROM SongCategories WHERE SongID = %s;", (song_id,))
                # Delete the song itself
                cur.execute("DELETE FROM Songs WHERE SongID = %s;", (song_id,))

        # Identify albums linked exclusively to these songs
        if song_ids:
            cur.execute("""
                    SELECT al.AlbumID, al.Title
                    FROM Albums al
                    LEFT JOIN SongAlbums sa ON al.AlbumID = sa.AlbumID
                    WHERE NOT EXISTS (
                        SELECT 1 FROM SongAlbums sa2
                        WHERE sa2.AlbumID = al.AlbumID AND sa2.SongID NOT IN %s
                    );
                """, (tuple(song_ids),))
            albums_for_deletion = cur.fetchall()
        else:
            albums_for_deletion = []

        for album in albums_for_deletion:
            album_id = album[0]
            # Delete album references
            cur.execute("DELETE FROM AlbumArtists WHERE AlbumID = %s;", (album_id,))
            cur.execute("DELETE FROM SongAlbums WHERE AlbumID = %s;", (album_id,))
            cur.execute("DELETE FROM Albums WHERE AlbumID = %s;", (album_id,))

        # Identify artists linked exclusively to these albums and songs
        if song_ids or albums_for_deletion:
            cur.execute("""
                    SELECT a.ArtistID, a.Name
                    FROM Artists a
                    LEFT JOIN SongArtists sa ON a.ArtistID = sa.ArtistID
                    LEFT JOIN AlbumArtists aa ON a.ArtistID = aa.ArtistID
                    WHERE NOT EXISTS (
                        SELECT 1 FROM SongArtists sa2
                        WHERE sa2.ArtistID = a.ArtistID AND sa2.SongID NOT IN %s
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM AlbumArtists aa2
                        WHERE aa2.ArtistID = a.ArtistID AND aa2.AlbumID NOT IN %s
                    );
                """, (tuple(song_ids), tuple([album[0] for album in albums_for_deletion])))
            artists_for_deletion = cur.fetchall()
        else:
            artists_for_deletion = []

        for artist in artists_for_deletion:
            artist_id = artist[0]
            # Delete the artist
            cur.execute("DELETE FROM Artists WHERE ArtistID = %s;", (artist_id,))

        # Finally, delete the category
        cur.execute("DELETE FROM Categories WHERE CategoryID = %s;", (category[0],))
        conn.commit()
        print("Category and related songs deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()  # Rollback in case of error
    finally:
        cur.execute("SET session_replication_role = DEFAULT;")

def delete_song(conn):
    with conn.cursor() as cur:
        # Display existing songs
        cur.execute("""
            SELECT s.SongID, s.Title, 
                   array_agg(DISTINCT a.Name) AS Artists,
                   array_agg(DISTINCT al.Title) AS Albums,
                   array_agg(DISTINCT c.Name) AS Categories
            FROM Songs s
            LEFT JOIN SongArtists sa ON s.SongID = sa.SongID
            LEFT JOIN Artists a ON sa.ArtistID = a.ArtistID
            LEFT JOIN SongAlbums sal ON s.SongID = sal.SongID
            LEFT JOIN Albums al ON sal.AlbumID = al.AlbumID
            LEFT JOIN SongCategories sc ON s.SongID = sc.SongID
            LEFT JOIN Categories c ON sc.CategoryID = c.CategoryID
            GROUP BY s.SongID
            ORDER BY s.Title;
        """)
        songs = cur.fetchall()

        if not songs:
            print("No songs found.")
            return

        print("Existing songs:")
        for s_row in songs:
            title = s_row[1]
            artist_list = ', '.join([art for art in s_row[2] if art]) if s_row[2] and s_row[2][0] is not None else "No Artists"
            album_list = ', '.join([alb for alb in s_row[3] if alb]) if s_row[3] and s_row[3][0] is not None else "No Albums"
            category_list = ', '.join([cat for cat in s_row[4] if cat]) if s_row[4] and s_row[4][0] is not None else "No Categories"
            print(f"- {title} | Artists: {artist_list} | Albums: {album_list} | Categories: {category_list}")

    song_title = input("Enter the title of the song to delete: ")
    with conn.cursor() as cur:
        cur.execute("SELECT SongID FROM Songs WHERE Title = %s;", (song_title,))
        song = cur.fetchone()

        if not song:
            print("Song not found.")
            return

        print(f"WARNING: Deleting the song '{song_title}' will remove it from all associated albums, artists, and categories.")
        print("Additionally, any artists or albums that have no other songs will also be deleted.")
        confirm = input("Do you want to proceed? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Deletion canceled.")
            return

        song_id = song[0]

        try:
            cur.execute("BEGIN;")
            cur.execute("SET session_replication_role = 'replica';")

            # Delete references from junction tables for this song
            cur.execute("DELETE FROM SongArtists WHERE SongID = %s;", (song_id,))
            cur.execute("DELETE FROM SongCategories WHERE SongID = %s;", (song_id,))
            cur.execute("DELETE FROM SongAlbums WHERE SongID = %s;", (song_id,))

            # Delete the song itself
            cur.execute("DELETE FROM Songs WHERE SongID = %s;", (song_id,))

            # Albums that have zero songs left
            cur.execute("""
                SELECT al.AlbumID
                FROM Albums al
                LEFT JOIN SongAlbums sa ON al.AlbumID = sa.AlbumID
                GROUP BY al.AlbumID
                HAVING COUNT(sa.SongID) = 0;
            """)
            orphan_albums = cur.fetchall()
            for album_row in orphan_albums:
                orphan_album_id = album_row[0]
                # Remove references to this album from AlbumArtists
                cur.execute("DELETE FROM AlbumArtists WHERE AlbumID = %s;", (orphan_album_id,))
                # Remove the album itself
                cur.execute("DELETE FROM Albums WHERE AlbumID = %s;", (orphan_album_id,))

            # Artists that have no songs remaining
            cur.execute("""
                SELECT a.ArtistID
                FROM Artists a
                LEFT JOIN SongArtists sa ON a.ArtistID = sa.ArtistID
                GROUP BY a.ArtistID
                HAVING COUNT(sa.SongID) = 0;
            """)
            orphan_artists = cur.fetchall()
            for artist_row in orphan_artists:
                orphan_artist_id = artist_row[0]
                # Remove references to this artist from AlbumArtists
                cur.execute("DELETE FROM AlbumArtists WHERE ArtistID = %s;", (orphan_artist_id,))
                # Remove the artist
                cur.execute("DELETE FROM Artists WHERE ArtistID = %s;", (orphan_artist_id,))

            cur.execute("COMMIT;")
            print(f"Song '{song_title}' and any orphaned albums/artists deleted successfully.")

        except Exception as e:
            cur.execute("ROLLBACK;")
            print(f"An error occurred: {e}")
        finally:
            cur.execute("SET session_replication_role = 'origin';")

def list_songs_by_artist(conn):
    print("Existing artists:")
    list_artists(conn)
    artist_name = input("Enter artist name to list songs: ")
    with conn.cursor() as cur:
        cur.execute("""
        SELECT s.Title
        FROM Songs s
        JOIN SongArtists sa ON s.SongID = sa.SongID
        JOIN Artists a ON sa.ArtistID = a.ArtistID
        WHERE a.Name = %s;
        """, (artist_name,))
        songs = cur.fetchall()
        if songs:
            print(f"Songs by {artist_name}:")
            for song in songs:
                print(f"- {song[0]}")
        else:
            print(f"No songs found for artist {artist_name}.")

def list_artists_with_albums_by_year(conn):
    year = input("Enter the year to list artists: ")
    with conn.cursor() as cur:
        cur.execute("""
        SELECT DISTINCT a.Name
        FROM Artists a
        JOIN AlbumArtists aa ON a.ArtistID = aa.ArtistID
        JOIN Albums al ON aa.AlbumID = al.AlbumID
        WHERE al.Year = %s;
        """, (year,))
        artists = cur.fetchall()
        if artists:
            print(f"Artists with albums released in {year}:")
            for artist in artists:
                print(f"- {artist[0]}")
        else:
            print(f"No artists found with albums released in {year}.")

def list_albums_by_category(conn):
    print("Existing categories:")
    list_categories(conn)
    category_name = input("Enter the category name to list albums: ")
    with conn.cursor() as cur:
        cur.execute("""
        SELECT al.Title, array_agg(DISTINCT a.Name) AS Artists
        FROM Albums al
        JOIN SongAlbums sa ON al.AlbumID = sa.AlbumID
        JOIN Songs s ON sa.SongID = s.SongID
        JOIN SongCategories sc ON s.SongID = sc.SongID
        JOIN Categories c ON sc.CategoryID = c.CategoryID
        JOIN AlbumArtists aa ON al.AlbumID = aa.AlbumID
        JOIN Artists a ON aa.ArtistID = a.ArtistID
        WHERE c.Name = %s
        GROUP BY al.AlbumID
        ORDER BY al.Title;
        """, (category_name,))
        albums = cur.fetchall()
        if not albums:
            print("No albums found containing songs in the category '{0}'.".format(category_name))
        else:
            print(f"Albums containing songs in the category '{category_name}':")
            for title, artists in albums:
                artist_names = ', '.join(set(artists))
                print(f"- {title} by {artist_names}")

def wipe_database(conn):
    print("WARNING: You are about to wipe the entire database. This action cannot be undone.")
    confirm = input("Type 'DELETE' to confirm: ")
    if confirm == 'DELETE':
        with conn.cursor() as cur:
            try:
                # Disable foreign key checks to allow deletion of all tables without constraint conflicts
                cur.execute("SET session_replication_role = 'replica';")

                # Order of deletion should respect foreign key constraints
                # Assuming SongAlbums, SongArtists, SongCategories, AlbumArtists are junction tables
                cur.execute("DELETE FROM SongCategories;")
                cur.execute("DELETE FROM SongAlbums;")
                cur.execute("DELETE FROM SongArtists;")
                cur.execute("DELETE FROM AlbumArtists;")

                # Deleting main entity tables
                cur.execute("DELETE FROM Songs;")
                cur.execute("DELETE FROM Albums;")
                cur.execute("DELETE FROM Artists;")
                cur.execute("DELETE FROM Categories;")

                # Commit changes to ensure all data is deleted
                conn.commit()
                print("Database wiped successfully.")
            except Exception as e:
                # Rollback in case of any error during deletion
                conn.rollback()
                print("Failed to wipe the database:", str(e))
            finally:
                # Re-enable foreign key checks
                cur.execute("SET session_replication_role = 'origin';")
    else:
        print("Database wipe canceled.")

def edit_artist(conn):
    with conn.cursor() as cur:
        # Show all available artists
        cur.execute("SELECT Name FROM Artists ORDER BY Name;")
        all_artists = cur.fetchall()
        if not all_artists:
            print("No artists found in the database.")
            return

        print("Available Artists:")
        for artist_row in all_artists:
            print(f"- {artist_row[0]}")

    artist_name = input("Enter the artist name to edit: ")
    with conn.cursor() as cur:
        cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (artist_name,))
        artist = cur.fetchone()
        if not artist:
            print("Artist not found.")
            return

        new_name = input("Enter the new artist name: ")
        if cur.fetchone():
            print(f"An artist with the name '{new_name}' already exists. Operation canceled.")
            return

        cur.execute("UPDATE Artists SET Name = %s WHERE ArtistID = %s;", (new_name, artist[0]))
        conn.commit()
        print("Artist name updated successfully.")

def edit_album(conn):
    with conn.cursor() as cur:
        # Show all available albums
        cur.execute("SELECT Title, Year FROM Albums ORDER BY Title;")
        all_albums = cur.fetchall()
        if not all_albums:
            print("No albums found in the database.")
            return

        print("Available Albums:")
        for album_row in all_albums:
            print(f"- {album_row[0]} (Year: {album_row[1]})")

    album_title = input("Enter the album title to edit: ")
    with conn.cursor() as cur:
        cur.execute("SELECT AlbumID, Title, Year FROM Albums WHERE Title = %s;", (album_title,))
        album = cur.fetchone()
        if not album:
            print("Album not found.")
            return

        # album = (AlbumID, Title, Year)
        album_id, current_title, current_year = album

        print("What would you like to edit?")
        print("1. Album title")
        print("2. Release year")
        choice = input("Enter choice (1 or 2): ")

        if choice == '1':
            new_title = input(f"Current title: '{current_title}'. Enter the new album title: ")
            cur.execute("SELECT 1 FROM Albums WHERE Title = %s;", (new_title,))
            if cur.fetchone():
                print(f"An album with the title '{new_title}' already exists. Operation canceled.")
                return
            cur.execute("UPDATE Albums SET Title = %s WHERE AlbumID = %s;", (new_title, album_id))
            print("Album title updated successfully.")
        elif choice == '2':
            new_year = input(f"Current year: '{current_year}'. Enter the new release year: ")
            cur.execute("UPDATE Albums SET Year = %s WHERE AlbumID = %s;", (new_year, album_id))
            print("Release year updated successfully.")
        else:
            print("Invalid choice. No changes made.")
            return

        conn.commit()

def edit_category(conn):
    with conn.cursor() as cur:
        # Show all available categories
        cur.execute("SELECT Name FROM Categories ORDER BY Name;")
        all_categories = cur.fetchall()
        if not all_categories:
            print("No categories found in the database.")
            return

        print("Available Categories:")
        for cat_row in all_categories:
            print(f"- {cat_row[0]}")

    category_name = input("Enter the category name to edit: ")
    with conn.cursor() as cur:
        cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (category_name,))
        category = cur.fetchone()
        if not category:
            print("Category not found.")
            return

        new_name = input("Enter the new category name: ")
        cur.execute("SELECT 1 FROM Categories WHERE Name = %s;", (new_name,))
        if cur.fetchone():
            print(f"A category with the name '{new_name}' already exists. Operation canceled.")
            return
        cur.execute("UPDATE Categories SET Name = %s WHERE CategoryID = %s;", (new_name, category[0]))
        conn.commit()
        print("Category name updated successfully.")

def edit_song(conn):
    with conn.cursor() as cur:
        # Show all available songs
        cur.execute("""
            SELECT s.Title
            FROM Songs s
            ORDER BY s.Title;
        """)
        all_songs = cur.fetchall()
        if not all_songs:
            print("No songs found in the database.")
            return

        print("Available Songs:")
        for song_row in all_songs:
            title = song_row[0]
            print(f"- {title}")

    song_title = input("Enter the song title to edit: ")
    with conn.cursor() as cur:
        cur.execute("SELECT SongID FROM Songs WHERE Title = %s;", (song_title,))
        song = cur.fetchone()
        if not song:
            print("Song not found.")
            return

        new_title = input("Enter the new song title: ")
        cur.execute("SELECT 1 FROM Songs WHERE Title = %s;", (new_title,))
        if cur.fetchone():
            print(f"A song with the title '{new_title}' already exists. Operation canceled.")
            return

        cur.execute("UPDATE Songs SET Title = %s WHERE SongID = %s;", (new_title, song[0]))
        conn.commit()
        print("Song name updated successfully.")

def main_menu():
    conn = connect_db()
    try:
        while True:
            print("\n1. Create Artist")
            print("2. Create Album")
            print("3. Create Category")
            print("4. Create Song")
            print("5. List Artists")
            print("6. List Albums")
            print("7. List Categories")
            print("8. List Songs")
            print("9. Delete Artist")
            print("10. Delete Album")
            print("11. Delete Category")
            print("12. Delete Song")
            print("13. List Songs by Artist")
            print("14. List Artists with Albums by Year")
            print("15. List Albums by Category")
            print("16. Wipe Entire Database")
            print("17. Edit Artist")
            print("18. Edit Album")
            print("19. Edit Category")
            print("20. Edit Song")
            print("0. Exit")

            choice = input("Enter choice: ")
            if choice == '1':
                create_artist(conn)
            elif choice == '2':
                create_album(conn)
            elif choice == '3':
                create_category(conn)
            elif choice == '4':
                create_song(conn)
            elif choice == '5':
                list_artists(conn)
            elif choice == '6':
                list_albums(conn)
            elif choice == '7':
                list_categories(conn)
            elif choice == '8':
                list_songs(conn)
            elif choice == '9':
                delete_artist(conn)
            elif choice == '10':
                delete_album(conn)
            elif choice == '11':
                delete_category(conn)
            elif choice == '12':
                delete_song(conn)
            elif choice == '13':
                list_songs_by_artist(conn)
            elif choice == '14':
                list_artists_with_albums_by_year(conn)
            elif choice == '15':
                list_albums_by_category(conn)
            elif choice == '16':
                wipe_database(conn)
            elif choice == '17':
                edit_artist(conn)
            elif choice == '18':
                edit_album(conn)
            elif choice == '19':
                edit_category(conn)
            elif choice == '20':
                edit_song(conn)
            elif choice == '0':
                break
            else:
                print("Invalid choice. Please try again.")
    finally:
        conn.close()

if __name__ == "__main__":
    main_menu()