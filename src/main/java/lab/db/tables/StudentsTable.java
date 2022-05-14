package lab.db.tables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.swing.plaf.basic.BasicGraphicsUtils;

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
				final int id = resultSet.getInt("id");
				final String firstName = resultSet.getString("firstName");
				final String lastName = resultSet.getString("lastName");
				final Optional<Date> birthday = Optional.ofNullable(Utils.sqlDateToDate(resultSet.getDate("birthday")));
				Student student = new Student(id, firstName, lastName, birthday);

				studentList.add(student);
			}
		} catch (final SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}

		return studentList;
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
		try (final Statement statement = this.connection.createStatement()) {
			final ResultSet resultSet = statement.executeQuery(query);
			return readStudentsFromResultSet(resultSet);
		} catch (final SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	public List<Student> findByBirthday(final Date date) {
		final String query = "SELECT * FROM " + TABLE_NAME + "WHERE date = ?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setDate(1, Utils.dateToSqlDate(date));
			final ResultSet resultSet = statement.executeQuery();
			return readStudentsFromResultSet(resultSet);
		} catch (final SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	@Override
	public boolean dropTable() {
		final String query = "DROP TABLE " + TABLE_NAME;
		try (final Statement statement = this.connection.createStatement()) {
			statement.executeUpdate(query);
			return true;
		} catch (final SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	@Override
	public boolean save(final Student student) {
		final String query = "INSERT INTO " + TABLE_NAME + "(id, firstName, lastName, birthday) VALUES (?, ?, ?, ?)";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setInt(1, student.getId());
			statement.setString(2, student.getFirstName());
			statement.setString(3, student.getLastName());
			statement.setDate(4, student.getBirthday().map(birthday -> Utils.dateToSqlDate(birthday)).orElse(null));
			statement.executeUpdate();
			return true;
		} catch (final SQLIntegrityConstraintViolationException e) {
			return false;
		} catch (final SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	@Override
	public boolean delete(final Integer id) {
		final String query = "DELETE FROM " + TABLE_NAME + "WHERE id = ?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setInt(1, id);
			return statement.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}

	@Override
	public boolean update(final Student student) {
		final String query = "UPDATE " + TABLE_NAME + " SET " + "firstName = ?," + "lastName = ?," + "birthday = ? "
				+ "WHERE id = ?";
		try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
			statement.setString(1, student.getFirstName());
			statement.setString(2, student.getLastName());
			statement.setDate(3, student.getBirthday().map(birthday -> Utils.dateToSqlDate(birthday)).orElse(null));
			statement.setInt(4, student.getId());
			return statement.executeUpdate() > 0;
		} catch (final SQLException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}
}