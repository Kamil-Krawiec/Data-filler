CREATE TABLE Authors (
    author_id SERIAL PRIMARY KEY,
    sex CHAR(1) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    birth_date DATE NOT NULL,

    CONSTRAINT unique_author_name UNIQUE (first_name, last_name)
);

CREATE TABLE Categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE Books (
    book_id SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    isbn VARCHAR(13) NOT NULL UNIQUE,
    author_id INT NOT NULL,
    publication_year INT NOT NULL,
    category_id INT NOT NULL,
    penalty_rate DECIMAL(5,2) NOT NULL,

    CONSTRAINT fk_books_author
        FOREIGN KEY(author_id)
        REFERENCES Authors(author_id),

    CONSTRAINT fk_books_category
        FOREIGN KEY(category_id)
        REFERENCES Categories(category_id),

    CONSTRAINT chk_isbn_format
        CHECK (isbn ~ '^\d{13}$'),

    CONSTRAINT chk_publication_year
        CHECK (publication_year >= 1900 AND publication_year <= EXTRACT(YEAR FROM CURRENT_DATE))
);

CREATE TABLE Members (
    member_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    registration_date DATE NOT NULL,

    CONSTRAINT chk_email_format
        CHECK (email ~ '^[\w\.-]+@[\w\.-]+\.\w{2,}$')
);

CREATE TABLE Loans (
    loan_id SERIAL PRIMARY KEY,
    book_id INT NOT NULL,
    member_id INT NOT NULL,
    loan_date DATE NOT NULL,
    due_date DATE NOT NULL,
    return_date DATE,

    CONSTRAINT fk_loans_book
        FOREIGN KEY(book_id)
        REFERENCES Books(book_id),

    CONSTRAINT fk_loans_member
        FOREIGN KEY(member_id)
        REFERENCES Members(member_id),

    CONSTRAINT chk_due_date
        CHECK (due_date > loan_date),

    CONSTRAINT chk_return_date
        CHECK (return_date IS NULL OR return_date > loan_date)
);

CREATE TABLE Penalties (
    penalty_id SERIAL PRIMARY KEY,
    loan_id INT NOT NULL,
    penalty_amount DECIMAL(10,2) NOT NULL,
    penalty_date DATE NOT NULL,

    CONSTRAINT fk_penalties_loan
        FOREIGN KEY(loan_id)
        REFERENCES Loans(loan_id),

    CONSTRAINT chk_penalty_amount
        CHECK (penalty_amount > 0)
);
