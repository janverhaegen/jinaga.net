using System;

namespace Jinaga.Test.Model
{
    [FactType("Skylane.Airline")]
    public class Airline
    {
        public string Identifier { get; set; }
    }
    [FactType("Skyline.Airline.Day")]
    public class AirlineDay
    {
        public Airline Airline { get; set; }
        public DateTime Date { get; set; }
    }
    [FactType("Skylane.Flight")]
    public class Flight
    {
        public AirlineDay AirlineDay { get; set; }
        public int FlightNumber { get; set; }
    }
    [FactType("Skylane.Flight.Cancellation")]
    public class FlightCancellation
    {
        public Flight Flight { get; set; }
        public DateTime DateCancelled { get; set; }
    }
}