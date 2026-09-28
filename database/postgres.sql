CREATE TABLE students (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  department VARCHAR(100) NOT NULL,
  age INTEGER
);

INSERT INTO students (name, department, age) VALUES ('Arun', 'CSE', 21);
INSERT INTO students (name, department, age) VALUES ('Priya', 'ECE', 22);
INSERT INTO students (name, department, age) VALUES ('Kavin', 'IT', 21);
