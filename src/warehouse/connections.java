package warehouse;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.time.Month;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.LocalDate;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;



public class connections {

	static final String DB_URL_METRO = "jdbc:mysql://localhost:3306/metro";
	static final String DB_URL_WAREHOUSE = "jdbc:mysql://localhost:3306/warehouse";
//	static final String USER = "root";
//	static final String PASS = "1234";

	public void setConnections() {

		
		Scanner sc= new Scanner(System.in);  
		
		System.out.print("Enter user name: \n");  
		String USER = sc.nextLine();               

		System.out.print("Enter Password : \n");  
		String PASS = sc.nextLine();                
		
		int bool = 0;
		int max_limit = 10000;

		String[][] master_array = new String[10][5];

		Queue<List<String>> queue = new LinkedList<>();

		MultiValuedMap<String, List> Mul_hash = new ArrayListValuedHashMap<String, List>();

		try {

			int master_limit = 0;
			int transaction_limit = 0;

			int master_iterator = 0;

			Connection c = DriverManager.getConnection(DB_URL_METRO, USER, PASS);
			Connection c1 = DriverManager.getConnection(DB_URL_WAREHOUSE, USER, PASS);
			Statement m = c.createStatement();

			while (bool == 0) {

//....................................... loading master data..............................
				ResultSet mr = m
						.executeQuery("SELECT * FROM masterdata ORDER BY PRODUCT_ID ASC LIMIT " + master_limit + ",10");
				master_limit += 10;

				if (master_limit == 100) {
					master_limit = 0;
				}

				while (mr.next()) {

					master_array[master_iterator][0] = mr.getString("PRODUCT_ID");
					master_array[master_iterator][1] = mr.getString("PRODUCT_NAME");
					master_array[master_iterator][2] = mr.getString("SUPPLIER_ID");
					master_array[master_iterator][3] = mr.getString("SUPPLIER_NAME");
					master_array[master_iterator][4] = mr.getString("PRICE");

					master_iterator++;
					if (master_iterator == 10) {
						master_iterator = 0;
					}

				}

//..................................... loading transactions data............................

				// List<String> Queue_list = new LinkedList<>();
				List<String> Queue_list = new ArrayList<String>();

				int transaction_iterator = 0;

				ResultSet tr = m.executeQuery("SELECT * FROM transactions LIMIT " + transaction_limit + ",50");

				transaction_limit += 50;

				while (tr.next()) {

					List<String> Transaction_list = new ArrayList<String>();

					Queue_list.add(tr.getString("PRODUCT_ID"));
					Queue_list.add(tr.getString("TRANSACTION_ID"));

					Transaction_list.add(tr.getString("TRANSACTION_ID"));
					Transaction_list.add(tr.getString("PRODUCT_ID"));
					Transaction_list.add(tr.getString("CUSTOMER_ID"));
					Transaction_list.add(tr.getString("CUSTOMER_NAME"));
					Transaction_list.add(tr.getString("STORE_ID"));
					Transaction_list.add(tr.getString("STORE_NAME"));
					Transaction_list.add(tr.getString("T_DATE"));
					Transaction_list.add(tr.getString("QUANTITY"));

					Mul_hash.put(tr.getString("PRODUCT_ID"), Transaction_list);

					transaction_iterator++;
					if (transaction_iterator == 50) {
						transaction_iterator = 0;
					}
				}

				queue.add(Queue_list);

//........................................... Matching section................................
				System.out.println("\n" + queue + "\n");

				for (int temp = 0; temp < 10; temp++) {
					String PID = master_array[temp][0];

					List<List> tempp = new ArrayList<List>(Mul_hash.get(PID));

					for (int i = 0; i < tempp.size(); i++) {
						// String s1 = (String) tempp.get(i).get(1);
						// System.out.println(s1);

//................................product dimension insertion
						try {
							Statement p = c1.createStatement();

							String query1 = "insert into products " + "values ('" + PID + "', '" + master_array[temp][1]
									+ "')";
							p.executeUpdate(query1);
						} catch (SQLException e) {

						}

//................................supplier dimension

						String Supplier_id = master_array[temp][2];
						String suppplier_name = master_array[temp][3];
						try {
							if (Supplier_id.equals("SP-15")) {
								suppplier_name = suppplier_name.replaceFirst("'", "");
							}

							Statement p = c1.createStatement();
							String query2 = "insert into suppliers " + "values ('" + Supplier_id + "', '"
									+ suppplier_name + "')";
							p.executeUpdate(query2);
						} catch (SQLException e) {

						}

//.......................store dimension

						String store_id = (String) tempp.get(i).get(4);
						String store_name = (String) tempp.get(i).get(5);

						try {
							Statement p = c1.createStatement();
							String query2 = "insert into stores " + "values ('" + store_id + "', '" + store_name + "')";
							p.executeUpdate(query2);
						} catch (SQLException e) {

						}

//.......................customer dimension

						String cust_id = (String) tempp.get(i).get(2);
						String cust_name = (String) tempp.get(i).get(3);

						try {
							Statement p = c1.createStatement();
							String query2 = "insert into customers " + "values ('" + cust_id + "', '" + cust_name
									+ "')";
							p.executeUpdate(query2);
						} catch (SQLException e) {

						}

//.........................date dimension
						// date
						String date = (String) tempp.get(i).get(6);
						LocalDate parsed = LocalDate.parse(date);

						// day
						int day = parsed.getDayOfMonth();

						// month
						Month month = parsed.getMonth();

						// year
						int year = parsed.getYear();

						// day of week
						SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
						Date dt = date_format.parse(date);
						DateFormat format_week = new SimpleDateFormat("EEEE");
						String day_of_week = format_week.format(dt);

						// month
						SimpleDateFormat month_date = new SimpleDateFormat("MMMM");
						String month_name = month_date.format(dt);

						// quarter
						LocalDate localDate = LocalDate.parse(date);
						int quarter = localDate.get(IsoFields.QUARTER_OF_YEAR);

						// week of year
						LocalDate date_for_week = LocalDate.of(year, month, day);
						int weekOfYear = date_for_week.get(ChronoField.ALIGNED_WEEK_OF_YEAR);

						// date id
						String ID = day + month_name + year;
						// System.out.println(" day " + day + " month " + month_name + " year " + year +
						// " day of week " + day_of_week + " quarter " + quarter + " week of year " +
						// weekOfYear + " id " + ID);

						// query
						try {
							Statement p = c1.createStatement();
							String query2 = "insert into dates " + "values ('" + ID + "', " + day + ", " + weekOfYear
									+ ", '" + month_name + "', " + quarter + ", " + year + ", '" + day_of_week + "')";
							p.executeUpdate(query2);
						} catch (SQLException e) {

						}

//............................fact table (sales)

						String tr_id = (String) tempp.get(i).get(0);
						int t_id = Integer.parseInt(tr_id);

						String price_str = (String) master_array[temp][4];
						float price = Float.parseFloat(price_str);

						String quantity_str = (String) tempp.get(i).get(7);
						int quantity = Integer.parseInt(quantity_str);

						float total_sale = price * quantity;

						// query
						try {
							Statement p = c1.createStatement();
							String query2 = "insert into sales " + "values (" + t_id + ", '" + store_id + "', '" + PID
									+ "', '" + Supplier_id + "', '" + cust_id + "', '" + ID + "', " + total_sale + ")";
							p.executeUpdate(query2);
						} catch (SQLException e) {

						}

					}

				}

//.............................................. removing section...................................

				// to check if queue is full(10 chunks ) or not
				int chunks = queue.size();
				if (chunks == 10) {
					System.out.println("queue is full");

					List<String> temprary_new = new ArrayList<String>(queue.peek());

					// each chunk have 50 tuple and total 10 chunk
					for (int del_chunk = 0; del_chunk < temprary_new.size(); del_chunk += 2) {

						String Q_PID = temprary_new.get(del_chunk);
						String Q_TID = temprary_new.get(del_chunk + 1);

						List<List> Hash_record = new ArrayList<List>(Mul_hash.get(Q_PID));
						for (int i = 0; i < Hash_record.size(); i++) {
							String H_TID = (String) Hash_record.get(i).get(0);
							String H_PID = (String) Hash_record.get(i).get(1);

							if (Q_TID.equals(H_TID)) {
								// System.out.println("Queue pid " + Q_PID + " Queue Tid " + Q_TID + " hash pid
								// " + H_PID + " hash t id " + H_TID);

								List<List> temp = new ArrayList<List>(Mul_hash.get(H_PID));
								List<String> r = new ArrayList<String>();
								r = temp.get(i);
								Mul_hash.removeMapping(H_PID, r);
								// System.out.println(r);
							}

						}
					}
					queue.remove();
				}


//...................................not empty ..............................

				if (transaction_limit == max_limit) {
					while (!queue.isEmpty()) {
						bool = 1;
						ResultSet second_mr = m.executeQuery(
								"SELECT * FROM masterdata ORDER BY PRODUCT_ID ASC LIMIT " + master_limit + ",10");
						master_limit += 10;

						if (master_limit == 100) {
							master_limit = 0;
						}

						while (second_mr.next()) {

							master_array[master_iterator][0] = second_mr.getString("PRODUCT_ID");
							master_array[master_iterator][1] = second_mr.getString("PRODUCT_NAME");
							master_array[master_iterator][2] = second_mr.getString("SUPPLIER_ID");
							master_array[master_iterator][3] = second_mr.getString("SUPPLIER_NAME");
							master_array[master_iterator][4] = second_mr.getString("PRICE");

							master_iterator++;
							if (master_iterator == 10) {
								master_iterator = 0;
							}

						}
//.....................matching...................
						for (int temp = 0; temp < 10; temp++) {
							String PID = master_array[temp][0];

							List<List> tempp = new ArrayList<List>(Mul_hash.get(PID));

							for (int i = 0; i < tempp.size(); i++) {
								// String s1 = (String) tempp.get(i).get(1);
								// System.out.println(s1);

								// ................................product dimension insertion
								try {
									Statement p = c1.createStatement();

									String query1 = "insert into products " + "values ('" + PID + "', '"
											+ master_array[temp][1] + "')";
									p.executeUpdate(query1);
								} catch (SQLException e) {

								}

								// ................................supplier dimension

								String Supplier_id = master_array[temp][2];
								String suppplier_name = master_array[temp][3];
								try {
									if (Supplier_id.equals("SP-15")) {
										suppplier_name = suppplier_name.replaceFirst("'", "");
									}

									Statement p = c1.createStatement();
									String query2 = "insert into suppliers " + "values ('" + Supplier_id + "', '"
											+ suppplier_name + "')";
									p.executeUpdate(query2);
								} catch (SQLException e) {

								}

								// .......................store dimension

								String store_id = (String) tempp.get(i).get(4);
								String store_name = (String) tempp.get(i).get(5);

								try {
									Statement p = c1.createStatement();
									String query2 = "insert into stores " + "values ('" + store_id + "', '" + store_name
											+ "')";
									p.executeUpdate(query2);
								} catch (SQLException e) {

								}

								// .......................customer dimension

								String cust_id = (String) tempp.get(i).get(2);
								String cust_name = (String) tempp.get(i).get(3);

								try {
									Statement p = c1.createStatement();
									String query2 = "insert into customers " + "values ('" + cust_id + "', '"
											+ cust_name + "')";
									p.executeUpdate(query2);
								} catch (SQLException e) {

								}

								// .........................date dimension
								// date
								String date = (String) tempp.get(i).get(6);
								LocalDate parsed = LocalDate.parse(date);

								// day
								int day = parsed.getDayOfMonth();

								// month
								Month month = parsed.getMonth();

								// year
								int year = parsed.getYear();

								// day of week
								SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
								Date dt = date_format.parse(date);
								DateFormat format_week = new SimpleDateFormat("EEEE");
								String day_of_week = format_week.format(dt);

								// month
								SimpleDateFormat month_date = new SimpleDateFormat("MMMM");
								String month_name = month_date.format(dt);

								// quarter
								LocalDate localDate = LocalDate.parse(date);
								int quarter = localDate.get(IsoFields.QUARTER_OF_YEAR);

								// week of year
								LocalDate date_for_week = LocalDate.of(year, month, day);
								int weekOfYear = date_for_week.get(ChronoField.ALIGNED_WEEK_OF_YEAR);

								// date id
								String ID = day + month_name + year;
								// System.out.println(" day " + day + " month " + month_name + " year " + year +
								// " day of week " + day_of_week + " quarter " + quarter + " week of year " +
								// weekOfYear + " id " + ID);

								// query
								try {
									Statement p = c1.createStatement();
									String query2 = "insert into dates " + "values ('" + ID + "', " + day + ", "
											+ weekOfYear + ", '" + month_name + "', " + quarter + ", " + year + ", '"
											+ day_of_week + "')";
									p.executeUpdate(query2);
								} catch (SQLException e) {

								}

								// ............................fact table (sales)

								String tr_id = (String) tempp.get(i).get(0);
								int t_id = Integer.parseInt(tr_id);

								String price_str = (String) master_array[temp][4];
								float price = Float.parseFloat(price_str);

								String quantity_str = (String) tempp.get(i).get(7);
								int quantity = Integer.parseInt(quantity_str);

								float total_sale = price * quantity;

								// query
								try {
									Statement p = c1.createStatement();
									String query2 = "insert into sales " + "values (" + t_id + ", '" + store_id + "', '"
											+ PID + "', '" + Supplier_id + "', '" + cust_id + "', '" + ID + "', "
											+ total_sale + ")";
									p.executeUpdate(query2);
								} catch (SQLException e) {

								}

							}
						}

						List<String> temprary_new = new ArrayList<String>(queue.peek());

						// each chunk have 50 tuple and total 10 chunk
						for (int del_chunk = 0; del_chunk < temprary_new.size(); del_chunk += 2) {

							String Q_PID = temprary_new.get(del_chunk);
							String Q_TID = temprary_new.get(del_chunk + 1);

							List<List> Hash_record = new ArrayList<List>(Mul_hash.get(Q_PID));
							for (int i = 0; i < Hash_record.size(); i++) {
								String H_TID = (String) Hash_record.get(i).get(0);
								String H_PID = (String) Hash_record.get(i).get(1);

								if (Q_TID.equals(H_TID)) {
									// System.out.println("Queue pid " + Q_PID + " Queue Tid " + Q_TID + " hash pid
									// " + H_PID + " hash t id " + H_TID);

									List<List> temp = new ArrayList<List>(Mul_hash.get(H_PID));
									List<String> r = new ArrayList<String>();
									r = temp.get(i);
									Mul_hash.removeMapping(H_PID, r);
									// System.out.println(r);
								}

							}
						}

						System.out.println(queue);
						queue.remove();

					}
					bool = 1;
				}
				System.out.println("\n\n\t\tEnded\n\n");

			}

			// print section


			// master queue

			System.out.println(queue);
			
			//hash table
			
			System.out.println("hash is " + Mul_hash);


		} catch (Exception exc) {

			System.out.println("Error while creating connection\nExisting...");
			//exc.printStackTrace();
		}
	}
}
