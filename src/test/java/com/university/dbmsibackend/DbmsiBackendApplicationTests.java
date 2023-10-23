package com.university.dbmsibackend;

import com.google.gson.Gson;
import com.university.dbmsibackend.domain.Attribute;
import com.university.dbmsibackend.domain.Database;
import com.university.dbmsibackend.domain.PrimaryKey;
import com.university.dbmsibackend.domain.Table;
import com.university.dbmsibackend.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class DbmsiBackendApplicationTests {

	@Test
	void contextLoads() throws IOException {
		Gson gson = new Gson();
		Foo foo = new Foo(1,"first", new ArrayList<>());

		String jsonStr = gson.toJson(foo);
		Foo result = gson.fromJson(jsonStr, Foo.class);
		result.getNumbers().add(2);
		result.getNumbers().add(3);
		result.getNumbers().add(4);
		String jsonStr2 = gson.toJson(result);
		System.out.println(jsonStr);
		System.out.println(result);
		System.out.println(jsonStr2);

//		FileWriter file = new FileWriter("src/test/java/com/university/dbmsibackend/output.json");
//		file.write(jsonStr2);
//		file.close();


//		JsonReader reader = new JsonReader(new FileReader("src/test/java/com/university/dbmsibackend/output.json"));
//		Foo data = gson.fromJson(reader, Foo.class);
//		System.out.println(data);

//		Database database = new Database();
//		database.setName("Students");
//		List<Database> databaseList = new ArrayList<>();
//
//		databaseList.add(database);
//		String databaseListJson = gson.toJson(databaseList);
//		System.out.println(databaseListJson);
//		JsonUtil.saveToCatalog(databaseListJson);
	}

	@Test
	void test() throws IOException {
		Gson gson = new Gson();
		List<Attribute> attributes = List.of(
				new Attribute("id", "int", 100, false),
				new Attribute("name", "string", 100, false),
				new Attribute("age", "int", 100, false),
				new Attribute("email", "string", 100, false)
		);
		List<Attribute> primary = List.of(
				new Attribute("id", "int", 100, false)
		);
		Table table = Table
				.builder()
				.name("students")
				.attributes(attributes)
				.primaryKeys(primary)
				.build();
		System.out.println(gson.toJson(table));
	}
}
