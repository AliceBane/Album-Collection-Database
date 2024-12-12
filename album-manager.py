import psycopg2

def connect_db():
    return psycopg2.connect(
        dbname="AlbumCollection",
        user="postgres",
        password="Fidiven03?",
        host="localhost"
    )

def create_artist(conn):
    name = input("Enter artist name: ")
    with conn.cursor() as cur:
        cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (name,))
        artist = cur.fetchone()
        if artist:
            print("Artist already exists.")
            return

        # Create the artist
        cur.execute("INSERT INTO Artists (Name) VALUES (%s) RETURNING ArtistID;", (name,))
        artist_id = cur.fetchone()[0]

        # Handle songs and their respective albums
        song_titles = input("Enter song titles separated by commas for the new artist: ").split(',')
        for song_title in song_titles:
            song_title = song_title.strip()
            album_titles = input(f"Enter album titles separated by commas for the song '{song_title}': ").split(',')
            categories = input(f"Enter category names separated by commas for the song '{song_title}': ").split(',')

            # Process each album mentioned for the song
            for album_title in album_titles:
                album_title = album_title.strip()
                cur.execute("SELECT AlbumID FROM Albums WHERE Title = %s;", (album_title,))
                album = cur.fetchone()

                if not album:
                    year = input(f"Enter the year the album '{album_title}' was released: ")
                    # Create the album and link it to the artist
                    cur.execute("INSERT INTO Albums (Title, Year, ArtistID) VALUES (%s, %s, %s) RETURNING AlbumID;", (album_title, year, artist_id))
                    album_id = cur.fetchone()[0]
                else:
                    album_id = album[0]

                # Check if the song already exists
                cur.execute("SELECT SongID FROM Songs WHERE Title = %s;", (song_title,))
                song = cur.fetchone()
                if not song:
                    cur.execute("INSERT INTO Songs (Title) VALUES (%s) RETURNING SongID;", (song_title,))
                    song_id = cur.fetchone()[0]
                else:
                    song_id = song[0]

                # Link song to artist and album
                cur.execute("SELECT * FROM SongArtists WHERE SongID = %s AND ArtistID = %s;", (song_id, artist_id))
                if not cur.fetchone():
                    cur.execute("INSERT INTO SongArtists (SongID, ArtistID) VALUES (%s, %s);", (song_id, artist_id))
                # Ensuring the song is linked to the album if not already linked
                cur.execute("SELECT 1 FROM SongAlbums WHERE SongID = %s AND AlbumID = %s;", (song_id, album_id))
                if not cur.fetchone():
                    cur.execute("INSERT INTO SongAlbums (SongID, AlbumID) VALUES (%s, %s);", (song_id, album_id))
                # Ensuring the artist is linked to the album (if not already linked)
                cur.execute("SELECT * FROM AlbumArtists WHERE AlbumID = %s AND ArtistID = %s;", (album_id, artist_id))
                if not cur.fetchone():
                    cur.execute("INSERT INTO AlbumArtists (AlbumID, ArtistID) VALUES (%s, %s);", (album_id, artist_id))

                # Handle categories for each song
                for category in categories:
                    category = category.strip()
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
    title = input("Enter album title: ")
    year = input("Enter the year the album was released: ")
    artist_names = input("Enter the artist names separated by commas for the album: ").split(',')
    song_titles = input("Enter song titles separated by commas for this album: ").split(',')

    with conn.cursor() as cur:
        # Check if the album already exists and get its ID or create a new one
        cur.execute("SELECT AlbumID FROM Albums WHERE Title = %s;", (title,))
        album = cur.fetchone()
        if not album:
            # Create the album if it does not exist
            cur.execute("INSERT INTO Albums (Title, Year) VALUES (%s, %s) RETURNING AlbumID;", (title, year))
            album_id = cur.fetchone()[0]
        else:
            album_id = album[0]

        # Process each artist linked to this album
        for artist_name in artist_names:
            artist_name = artist_name.strip()
            cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (artist_name,))
            artist = cur.fetchone()
            if not artist:
                # Create the artist if they do not exist
                cur.execute("INSERT INTO Artists (Name) VALUES (%s) RETURNING ArtistID;", (artist_name,))
                artist_id = cur.fetchone()[0]
            else:
                artist_id = artist[0]

            # Link the album with the artist in the AlbumArtists table
            cur.execute("INSERT INTO AlbumArtists (AlbumID, ArtistID) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (album_id, artist_id))

        # Process each song linked to this album
        for song_title in song_titles:
            song_title = song_title.strip()
            category_names = input(f"Enter category names separated by commas for the song '{song_title}': ").split(',')

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
                category_name = category_name.strip()
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

def create_song(conn):
    title = input("Enter song title: ")
    artist_names = input("Enter the artist names separated by commas for the song: ").split(',')
    album_titles = input("Enter the album titles separated by commas this song belongs to: ").split(',')
    category_names = input("Enter category names separated by commas for the song: ").split(',')

    with conn.cursor() as cur:
        # Insert the song
        cur.execute("INSERT INTO Songs (Title) VALUES (%s) RETURNING SongID;", (title,))
        song_id = cur.fetchone()[0]

        # Handle artists
        for artist_name in artist_names:
            artist_name = artist_name.strip()
            cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (artist_name,))
            artist = cur.fetchone()
            if not artist:
                cur.execute("INSERT INTO Artists (Name) VALUES (%s) RETURNING ArtistID;", (artist_name,))
                artist_id = cur.fetchone()[0]
            else:
                artist_id = artist[0]
            # Link song to artist
            cur.execute("INSERT INTO SongArtists (SongID, ArtistID) VALUES (%s, %s);", (song_id, artist_id))

        # Handle albums
        for album_title in album_titles:
            album_title = album_title.strip()
            cur.execute("SELECT AlbumID FROM Albums WHERE Title = %s;", (album_title,))
            album = cur.fetchone()
            if not album:
                year = input(f"Enter the year the album '{album_title}' was released: ")
                # Prompt for artists for this new album
                album_artist_names = input(
                    f"Enter artist names for the album '{album_title}', separated by commas: ").split(',')
                cur.execute("INSERT INTO Albums (Title, Year) VALUES (%s, %s) RETURNING AlbumID;", (album_title, year))
                album_id = cur.fetchone()[0]
                # Assign artists to this album
                for name in album_artist_names:
                    name = name.strip()
                    cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (name,))
                    album_artist = cur.fetchone()
                    if not album_artist:
                        cur.execute("INSERT INTO Artists (Name) VALUES (%s) RETURNING ArtistID;", (name,))
                        album_artist_id = cur.fetchone()[0]
                        print(f"Artist '{name}' created for album '{album_title}'.")
                    else:
                        album_artist_id = album_artist[0]
                    cur.execute("INSERT INTO AlbumArtists (AlbumID, ArtistID) VALUES (%s, %s);",
                                (album_id, album_artist_id))
            else:
                album_id = album[0]
            # Link song to album
            cur.execute("INSERT INTO SongAlbums (SongID, AlbumID) VALUES (%s, %s);", (song_id, album_id))

        # Handle categories
        for name in category_names:
            name = name.strip()
            cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (name,))
            category = cur.fetchone()
            if not category:
                cur.execute("INSERT INTO Categories (Name) VALUES (%s) RETURNING CategoryID;", (name,))
                category_id = cur.fetchone()[0]
                print(f"Category '{name}' created successfully.")
            else:
                category_id = category[0]
            cur.execute("INSERT INTO SongCategories (SongID, CategoryID) VALUES (%s, %s);", (song_id, category_id))

        conn.commit()
        print(f"Song '{title}' created successfully.")

def create_category(conn):
    name = input("Enter category name: ")
    with conn.cursor() as cur:
        cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (name,))
        if cur.fetchone():
            print("Category already exists.")
            return False
        cur.execute("INSERT INTO Categories (Name) VALUES (%s) RETURNING CategoryID;", (name,))
        category_id = cur.fetchone()[0]
        conn.commit()
        print(f"Category '{name}' created successfully.")
        return True

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
        # List existing artists for user selection
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

            # Step 1: Identify and delete albums exclusively linked to this artist
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

                # Step 2: Identify and delete songs exclusively linked to this album
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

            # Step 3: Delete references to the artist in junction tables
            cur.execute("DELETE FROM AlbumArtists WHERE ArtistID = %s;", (artist_id,))
            cur.execute("DELETE FROM SongArtists WHERE ArtistID = %s;", (artist_id,))

            # Step 4: Delete the artist
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

    # Start transaction block explicitly
    cur.execute("BEGIN;")

    try:
        # First, remove the album from the AlbumArtists junction table
        cur.execute("DELETE FROM AlbumArtists WHERE AlbumID = %s;", (album[0],))

        # Next, identify songs that are only linked to this album
        cur.execute("""
            SELECT SongID FROM SongAlbums WHERE AlbumID = %s
        """, (album[0],))
        song_ids_for_deletion = [song[0] for song in cur.fetchall()]

        # Remove these songs from the SongAlbums table
        cur.execute("DELETE FROM SongAlbums WHERE AlbumID = %s;", (album[0],))

        # For each song only linked to this deleted album, clear out entries from related tables
        for song_id in song_ids_for_deletion:
            # Check if these songs are still linked to any albums
            cur.execute("SELECT COUNT(*) FROM SongAlbums WHERE SongID = %s;", (song_id,))
            if cur.fetchone()[0] == 0:
                # Delete references from SongArtists
                cur.execute("DELETE FROM SongArtists WHERE SongID = %s;", (song_id,))
                # Delete references from SongCategories
                cur.execute("DELETE FROM SongCategories WHERE SongID = %s;", (song_id,))
                # Finally, delete the song itself if it's not linked to any other album
                cur.execute("DELETE FROM Songs WHERE SongID = %s;", (song_id,))

        # Finally, delete the album
        cur.execute("DELETE FROM Albums WHERE AlbumID = %s;", (album[0],))

        # Commit the transaction
        cur.execute("COMMIT;")
        print("Album and any exclusive songs and artists successfully deleted.")
    except Exception as e:
        print(f"An error occurred: {e}")
        cur.execute("ROLLBACK;")  # Rollback the transaction on error
    finally:
        # Re-enable foreign key checks
        cur.execute("SET session_replication_role = 'origin';")

def delete_category(conn):
    cur = conn.cursor()
    try:
        # Disable foreign key checks
        cur.execute("SET session_replication_role = replica;")

        # Display existing categories
        cur.execute("SELECT CategoryID, Name FROM Categories;")
        categories = cur.fetchall()
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

        # Check songs that only belong to this category
        cur.execute("""
            SELECT s.SongID
            FROM Songs s
            INNER JOIN SongCategories sc ON s.SongID = sc.SongID
            WHERE sc.CategoryID = %s
            AND NOT EXISTS (
                SELECT 1 FROM SongCategories sc2
                WHERE sc2.SongID = s.SongID AND sc2.CategoryID != %s
            );
        """, (category[0], category[0]))
        songs = cur.fetchall()

        if songs:
            song_ids = [song[0] for song in songs]
            print("Deleting category '" + category_name + "' will also delete these songs:")
            for song_id in song_ids:
                cur.execute("SELECT Title FROM Songs WHERE SongID = %s;", (song_id,))
                song_title = cur.fetchone()[0]
                print(f"- {song_title}")

            confirm = input("Do you still want to proceed with deleting this category and the listed songs? (yes/no): ")
            if confirm.lower() != 'yes':
                print("Deletion canceled.")
                return

            # Delete references from songalbums, songartists, and songcategories
            cur.execute("DELETE FROM SongAlbums WHERE SongID = ANY(%s);", (song_ids,))
            cur.execute("DELETE FROM SongArtists WHERE SongID = ANY(%s);", (song_ids,))
            cur.execute("DELETE FROM SongCategories WHERE SongID = ANY(%s);", (song_ids,))

            # Delete the songs themselves
            cur.execute("DELETE FROM Songs WHERE SongID = ANY(%s);", (song_ids,))

        # Finally, delete the category
        cur.execute("DELETE FROM Categories WHERE CategoryID = %s;", (category[0],))
        conn.commit()
        print("Category and related songs deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()  # Rollback in case of error
    finally:
        # Re-enable foreign key checks
        cur.execute("SET session_replication_role = DEFAULT;")

def list_songs_by_artist(conn):
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
    artist_name = input("Enter the artist name to edit: ")
    with conn.cursor() as cur:
        cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (artist_name,))
        artist = cur.fetchone()
        if not artist:
            print("Artist not found.")
            return

        print("Select what you would like to edit:")
        print("1. Artist name")
        print("2. Associated albums")
        choice = input("Enter your choice: ")

        if choice == '1':
            new_name = input("Enter the new artist name: ")
            cur.execute("UPDATE Artists SET Name = %s WHERE ArtistID = %s;", (new_name, artist[0]))
            print("Artist name updated successfully.")

        elif choice == '2':
            print("Current albums by this artist:")
            cur.execute("SELECT Title FROM Albums WHERE ArtistID = %s;", (artist[0],))
            albums = cur.fetchall()
            for album in albums:
                print(album[0])
            album_titles = input("Enter new comma-separated album titles for this artist: ").split(',')
            cur.execute("DELETE FROM Albums WHERE ArtistID = %s;", (artist[0],))
            for album_title in album_titles:
                cur.execute("INSERT INTO Albums (Title, ArtistID) VALUES (%s, %s);", (album_title.strip(), artist[0]))
            print("Associated albums updated successfully.")

        conn.commit()

def edit_album(conn):
    album_title = input("Enter the album title to edit: ")
    with conn.cursor() as cur:
        cur.execute("SELECT AlbumID FROM Albums WHERE Title = %s;", (album_title,))
        album = cur.fetchone()
        if not album:
            print("Album not found.")
            return

        print("Select what you would like to edit:")
        print("1. Album title")
        print("2. Release year")
        print("3. Associated songs")
        print("4. Associated artists")
        choice = input("Enter your choice: ")

        if choice == '1':
            new_title = input("Enter the new album title: ")
            cur.execute("UPDATE Albums SET Title = %s WHERE AlbumID = %s;", (new_title, album[0]))
            print("Album title updated successfully.")

        elif choice == '2':
            new_year = input("Enter the new release year: ")
            cur.execute("UPDATE Albums SET Year = %s WHERE AlbumID = %s;", (new_year, album[0]))
            print("Release year updated successfully.")

        elif choice == '3':
            print("Current songs in the album:")
            cur.execute("SELECT Title FROM Songs WHERE AlbumID = %s;", (album[0],))
            songs = cur.fetchall()
            for song in songs:
                print(song[0])
            song_titles = input("Enter new comma-separated song titles for this album: ").split(',')
            cur.execute("DELETE FROM Songs WHERE AlbumID = %s;", (album[0],))
            for song_title in song_titles:
                cur.execute("INSERT INTO Songs (Title, AlbumID) VALUES (%s, %s);", (song_title.strip(), album[0]))
            print("Associated songs updated successfully.")

        elif choice == '4':
            print("Current artists for this album:")
            cur.execute(
                "SELECT a.Name FROM Artists a JOIN AlbumArtists aa ON a.ArtistID = aa.ArtistID WHERE aa.AlbumID = %s;",
                (album[0],))
            artists = cur.fetchall()
            for artist in artists:
                print(artist[0])
            artist_names = input("Enter new comma-separated artist names for this album: ").split(',')
            cur.execute("DELETE FROM AlbumArtists WHERE AlbumID = %s;", (album[0],))
            for artist_name in artist_names:
                cur.execute("SELECT ArtistID FROM Artists WHERE Name = %s;", (artist_name.strip(),))
                artist = cur.fetchone()
                if not artist:
                    cur.execute("INSERT INTO Artists (Name) VALUES (%s) RETURNING ArtistID;", (artist_name.strip(),))
                    artist_id = cur.fetchone()[0]
                else:
                    artist_id = artist[0]
                cur.execute("INSERT INTO AlbumArtists (AlbumID, ArtistID) VALUES (%s, %s);", (album[0], artist_id))
            print("Associated artists updated successfully.")

        conn.commit()

def edit_category(conn):
    category_name = input("Enter the category name to edit: ")
    with conn.cursor() as cur:
        cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (category_name,))
        category = cur.fetchone()
        if not category:
            print("Category not found.")
            return

        new_name = input("Enter the new category name: ")
        cur.execute("UPDATE Categories SET Name = %s WHERE CategoryID = %s;", (new_name, category[0]))
        print("Category name updated successfully.")
        conn.commit()

def edit_song(conn):
    song_title = input("Enter the song title to edit: ")
    with conn.cursor() as cur:
        cur.execute("SELECT SongID FROM Songs WHERE Title = %s;", (song_title,))
        song = cur.fetchone()
        if not song:
            print("Song not found.")
            return

        print("Select what you would like to edit:")
        print("1. Song name")
        print("2. Song category")
        choice = input("Enter your choice: ")

        if choice == '1':
            new_title = input("Enter the new song title: ")
            cur.execute("UPDATE Songs SET Title = %s WHERE SongID = %s;", (new_title, song[0]))
            print("Song name updated successfully.")

        elif choice == '2':
            print("Current categories for this song:")
            cur.execute(
                "SELECT Name FROM Categories JOIN SongCategories ON Categories.CategoryID = SongCategories.CategoryID WHERE SongCategories.SongID = %s;",
                (song[0],))
            categories = cur.fetchall()
            for category in categories:
                print(category[0])
            category_names = input("Enter new comma-separated category names for this song: ").split(',')
            cur.execute("DELETE FROM SongCategories WHERE SongID = %s;", (song[0],))
            for category_name in category_names:
                cur.execute("SELECT CategoryID FROM Categories WHERE Name = %s;", (category_name.strip(),))
                category = cur.fetchone()
                if not category:
                    cur.execute("INSERT INTO Categories (Name) VALUES (%s) RETURNING CategoryID;",
                                (category_name.strip(),))
                    category_id = cur.fetchone()[0]
                else:
                    category_id = category[0]
                cur.execute("INSERT INTO SongCategories (SongID, CategoryID) VALUES (%s, %s);", (song[0], category_id))
            print("Song categories updated successfully.")

        conn.commit()

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