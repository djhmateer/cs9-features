using System;
using System.Collections.Generic;
using Dapper;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using ConsoleApp1;
using CsvHelper;

// this is in ConsoleApp1 namespace in the Thing Class
var thing = new Thing();
thing.Name = "Bob";

Thing thing2 = new Thing { Name = "Alice" };

using var db = GetOpenConnection();

// Clear down db first (using Dapper)
db.Execute("DELETE FROM Actors");

// 1.Extract Actors
var actors = LoadActorsFromCsv();
Console.WriteLine($"Total Actors imported from csv is {actors.Count}"); // 98,690

// 2.Load
foreach (var actor in actors.Take(50))
{
    var sql = @"
        INSERT Actors
        VALUES (@actorid, @name, @sex)";

    db.Execute(sql, actor);
}

// a test async query
var someActors = await db.QueryAsync<Actor>(@"
    SELECT TOP 10 * 
    FROM Actors 
    ORDER BY Name DESC");

foreach (var someActor in someActors)
    Console.WriteLine($"{someActor.actorid} {someActor.name} {someActor.sex}");

Console.WriteLine("Done");



// local function using CsvHelper to read a csv
List<Actor> LoadActorsFromCsv()
{
    using var reader = new StreamReader("..\\..\\..\\..\\data\\actors.csv");
    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
    csv.Configuration.Delimiter = ";";
    return csv.GetRecords<Actor>().ToList();
}

// a local function to help Dapper
IDbConnection GetOpenConnection()
{
    var connStrng = @"Server=.\;Database=IMDBChallenge;Trusted_Connection=True;MultipleActiveResultSets=true";
    //var connStrng = @"Server=(localdb)\mssqllocaldb;Database=IMDBChallenge;Trusted_Connection=True;MultipleActiveResultSets=true";
    var connection = new SqlConnection(connStrng);
    return connection;
}

// types must be defined at the bottom of the file
class Actor
{
    // favouring the simplest data type string in this load
    // until I understand the data (ie what edge cases are there)
    // yikes - naming convention need to refactor!
    public string actorid { get; set; }
    public string name { get; set; }
    public string sex { get; set; }
}

