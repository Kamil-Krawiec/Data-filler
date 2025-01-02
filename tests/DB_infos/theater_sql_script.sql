CREATE TABLE IF NOT EXISTS Theaters (
    theater_id SERIAL PRIMARY KEY,
    name VARCHAR(10) NOT NULL,
    capacity INT DEFAULT 0,

    CONSTRAINT capacity_positive CHECK (capacity>0 AND capacity<200)
);


CREATE TABLE Seats (
    row INT NOT NULL,
    seat INT NOT NULL,
    theater_id BIGINT UNSIGNED NOT NULL,
    CONSTRAINT unique_row_seat UNIQUE (row, seat, theater_id),
    CONSTRAINT fk_seats_theaters FOREIGN KEY (theater_id) REFERENCES Theaters(theater_id)
);


CREATE TABLE Movies (
    movie_id SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    director VARCHAR(100) NOT NULL,
    duration INT NOT NULL,
    penalty_rate DECIMAL(5,2) NOT NULL,

    CONSTRAINT chk_duration
        CHECK (duration >= 60 AND duration <=200)

);

CREATE TABLE Shows (
    show_id SERIAL PRIMARY KEY,
    theater_id BIGINT UNSIGNED  NOT NULL,
    movie_id  BIGINT UNSIGNED NOT NULL,
    show_date date NOT NULL,
    show_starts_at time NOT NULL,

    CONSTRAINT fk_shows_movies
        FOREIGN KEY(movie_id)
        REFERENCES Movies(movie_id),

    CONSTRAINT fk_shows_theaters
        FOREIGN KEY(theater_id)
        REFERENCES Theaters(theater_id)

);

CREATE TABLE Tickets (
   show_id BIGINT UNSIGNED NOT NULL,
    row INT NOT NULL,
    seat INT NOT NULL,
    theater_id BIGINT UNSIGNED NOT NULL,
price decimal(8,2) DEFAULT 0,

  CONSTRAINT fk_tickets_shows FOREIGN KEY (show_id) REFERENCES Shows(show_id),
  CONSTRAINT fk_tickets_seat FOREIGN KEY (row, seat, theater_id) REFERENCES Seats(row, seat, theater_id)
);