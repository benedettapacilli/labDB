package lab.db.tables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lab.db.Table;
import lab.model.Student;
import lab.utils.Utils;

public final class StudentsTable implements Table<Student, Integer> {
	public static final String TABLE_NAME = "students";

	private final Connection connection;

	public StudentsTable(final Connection connection) {
		this.connection = Objects.requireNonNull(connection);
	}

	@Override
	public String getTableName() {
		return TABLE_NAME;
	}

	@Override
	public boolean createTable() {
		// 1. Create the statement from the open connection inside a try-with-resources
		try (final Statement statement = this.connection.createStatement()) {
			// 2. Execute the statement with the given query
			statement.executeUpdate("CREATE TABLE " + TABLE_NAME + " (" + "id INT NOT NULL PRIMARY KEY,"
					+ "firstName CHAR(40)," + "lastName CHAR(40)," + "birthday DATE" + ")");
			return true;
		} catch (final SQLException e) {
			// 3. Handle possible SQLExceptions
			return false;
		}
	}

	@Override
	public Optional<Student> findByPrimaryKey(final Integer id) {
		final String query = "SELECT * FROM " + TABLE_NAME + "wHERE id = ?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setInt(1, id);
			final ResultSet resultSet = statement.executeQuery();
			return readStudentsFromResultSet(resultSet).stream().findFirst();
		} catch (final SQLException e) {
			return Optional.empty();
		}
	}

	/**
	 * Given a ResultSet read all the students in it and collects them in a List
	 * 
	 * @param resultSet a ResultSet from which the Student(s) will be extracted
	 * @return a List of all the students in the ResultSet
	 */
	private List<Student> readStudentsFromResultSet(final ResultSet resultSet) {
		List<Student> studentList = new ArrayList<>();

		try {
			while (resultSet.next()) {
				int id;
				String firstName;
				String lastName;
				Student student = new Student(id, firstName, lastName);

				studentList.add(student);
			}
			return studentList;
		} catch (final SQLException e) {
			e.printStackTrace();
			return null;
		}
		// Create an empty list, then
		// Inside a loop you should:
		// 1. Call resultSet.next() to advance the pointer and check there are still
		// rows to fetch
		// 2. Use the getter methods to get the value of the columns
		// 3. After retrieving all the data create a Student object
		// 4. Put the student in the List
		// Then return the list with all the found students

		// Helpful resources:
		// https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html
		// https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html
	}

	@Override
	public List<Student> findAll() {
		final String query = "SELECT * FROM " + TABLE_NAME;
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			final ResultSet resultSet = statement.executeQuery();
			return readStudentsFromResultSet(resultSet);
		} catch (final SQLException e) {
			return null;
		}
	}

	public List<Student> findByBirthday(final Date date) {
		final String query = "SELECT * FROM " + TABLE_NAME + "WHERE date = ?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setDate(1, Utils.dateToSqlDate(date));
			final ResultSet resultSet = statement.executeQuery();
			return readStudentsFromResultSet(resultSet);
		} catch (final SQLException e) {
			return null;
		}
	}

	@Override
	public boolean dropTable() {
		final String query = "DROP TABLE " + TABLE_NAME;
		try (final Statement statement = this.connection.createStatement()) {
			statement.executeUpdate(query);
			return true;
		} catch (final SQLException e) {
			return false;
		}
	}

	@Override
	public boolean save(final Student student) {
		final String query = "INSERT INTO " + TABLE_NAME + " VALUE (" + student + ")";
		try (final Statement statement = this.connection.createStatement()) {
			statement.executeUpdate(query);
			return true;
		} catch (final SQLException e) {
			return false;
		}
	}

	@Override
	public boolean delete(final Integer id) {
		final String query = "SELECT * FROM " + TABLE_NAME + "WHERE is = ?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setInt(1, id);
			final ResultSet resultSet = statement.executeQuery();
			readStudentsFromResultSet(resultSet);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean update(final Student student) {
		throw new UnsupportedOperationException("TODO");
	}
}