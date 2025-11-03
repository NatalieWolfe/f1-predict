#include "data/constants_maps.h"

#include <string_view>
#include <unordered_map>

#include "data/constants.pb.h"

namespace f1_predict {
namespace {

const std::unordered_map<std::string_view, constants::Circuit>
    NAME_TO_CIRCUIT_MAP = {
        {"Australia", constants::AUSTRALIA_CIRCUIT},
        {"China", constants::CHINA_CIRCUIT},
        {"Japan", constants::JAPAN_CIRCUIT},
        {"Bahrain", constants::BAHRAIN_CIRCUIT},
        {"Saudi Arabia", constants::SAUDI_ARABIA_CIRCUIT},
        {"Miami", constants::MIAMI_CIRCUIT},
        {"Emilia-Romagna", constants::EMILIA_ROMAGNA_CIRCUIT},
        {"Emilia Romagna", constants::EMILIA_ROMAGNA_CIRCUIT},
        {"Monaco", constants::MONACO_CIRCUIT},
        {"Spain", constants::SPAIN_CIRCUIT},
        {"Canada", constants::CANADA_CIRCUIT},
        {"Austria", constants::AUSTRIA_CIRCUIT},
        {"Great Britain", constants::GREAT_BRITAIN_CIRCUIT},
        {"Belgium", constants::BELGIUM_CIRCUIT},
        {"Hungary", constants::HUNGARY_CIRCUIT},
        {"Netherlands", constants::NETHERLANDS_CIRCUIT},
        {"Italy", constants::ITALY_CIRCUIT},
        {"Azerbaijan", constants::AZERBAIJAN_CIRCUIT},
        {"Singapore", constants::SINGAPORE_CIRCUIT},
        {"United States", constants::UNITED_STATES_CIRCUIT},
        {"Mexico", constants::MEXICO_CIRCUIT},
        {"Brazil", constants::BRAZIL_CIRCUIT},
        {"Las Vegas", constants::LAS_VEGAS_CIRCUIT},
        {"Qatar", constants::QATAR_CIRCUIT},
        {"Abu Dhabi", constants::ABU_DHABI_CIRCUIT},
        {"France", constants::FRANCE_CIRCUIT},
        {"Portugal", constants::PORTUGAL_CIRCUIT},
        {"Styria", constants::STYRIA_CIRCUIT},
        {"Russia", constants::RUSSIA_CIRCUIT},
        {"Turkiye", constants::TURKIYE_CIRCUIT},
        {"Turkey", constants::TURKIYE_CIRCUIT}};

const std::unordered_map<std::string_view, constants::Team> NAME_TO_TEAM_MAP = {
    {"McLaren", constants::MCLAREN},
    {"McLaren Mercedes", constants::MCLAREN},
    {"Ferrari", constants::FERRARI},
    {"Red Bull Racing", constants::RED_BULL_RACING},
    {"Red Bull Racing Honda RBPT", constants::RED_BULL_RACING},
    {"Red Bull Racing honda RBPT", constants::RED_BULL_RACING},
    {"Red Bull Racing Honda EBPT", constants::RED_BULL_RACING},
    {"Red Bull Racing RBPT", constants::RED_BULL_RACING},
    {"Red Bull Racing Honda", constants::RED_BULL_RACING},
    {"Mercedes", constants::MERCEDES},
    {"Aston Martin", constants::ASTON_MARTIN},
    {"Aston Martin Aramco Mercedes", constants::ASTON_MARTIN},
    {"Aston Martin Mercedes", constants::ASTON_MARTIN},
    {"Alpine", constants::ALPINE},
    {"Alpine Renault", constants::ALPINE},
    {"Haas", constants::HAAS},
    {"Haas Ferrari", constants::HAAS},
    {"RB", constants::RB},
    {"Racing Bulls Honda RBPT", constants::RB},
    {"Racing bulls Honda RBPT", constants::RB},
    {"Racing Honda RBPT", constants::RB},
    {"RB Honda RBPT", constants::RB},
    {"AlphaTauri", constants::RB},
    {"AlphaTauri RBPT", constants::RB},
    {"AlphaTauri Honda RBPT", constants::RB},
    {"AlphaTauri Honda", constants::RB},
    {"Williams", constants::WILLIAMS},
    {"Williams Mercedes", constants::WILLIAMS},
    {"Kick Sauber", constants::KICK_SAUBER},
    {"Kick Sauber Ferrari", constants::KICK_SAUBER},
    {"Alfa Romeo Ferrari", constants::KICK_SAUBER},
    {"Alfa Romeo Racing Ferrari", constants::KICK_SAUBER}};

const std::unordered_map<std::string_view, constants::Driver>
    NAME_TO_DRIVER_MAP = {
        {"Alexander Albon", constants::ALEXANDER_ALBON},
        {"Carlos Sainz", constants::CARLOS_SAINZ},
        {"Charles Leclerc", constants::CHARLES_LECLERC},
        {"Esteban Ocon", constants::ESTEBAN_OCON},
        {"Fernando Alonso", constants::FERNANDO_ALONSO},
        {"Gabriel Bortoleto", constants::GABRIEL_BORTOLETO},
        {"George Russell", constants::GEORGE_RUSSELL},
        {"Isack Hadjar", constants::ISACK_HADJAR},
        {"Jack Doohan", constants::JACK_DOOHAN},
        {"Kimi Antonelli", constants::KIMI_ANTONELLI},
        {"Lance Stroll", constants::LANCE_STROLL},
        {"Lando Norris", constants::LANDO_NORRIS},
        {"Lewis Hamilton", constants::LEWIS_HAMILTON},
        {"Liam Lawson", constants::LIAM_LAWSON},
        {"Max Verstappen", constants::MAX_VERSTAPPEN},
        {"Nico Hulkenberg", constants::NICO_HULKENBERG},
        {"Oliver Bearman", constants::OLIVER_BEARMAN},
        {"Oscar Piastri", constants::OSCAR_PIASTRI},
        {"Pierre Gasly", constants::PIERRE_GASLY},
        {"Yuki Tsunoda", constants::YUKI_TSUNODA},
        {"Franco Colapinto", constants::FRANCO_COLAPINTO},
        {"Valtteri Bottas", constants::VALTTERI_BOTTAS},
        {"Sergio Perez", constants::SERGIO_PEREZ},
        {"Kevin Magnussen", constants::KEVIN_MAGNUSSEN},
        {"Guanyu Zhou", constants::GUANYU_ZHOU},
        {"Daniel Ricciardo", constants::DANIEL_RICCIARDO},
        {"Logan Sargeant", constants::LOGAN_SARGEANT},
        {"Nyck De Vries", constants::NYCK_DE_VRIES},
        {"Sebastian Vettel", constants::SEBASTIAN_VETTEL},
        {"Mick Schumacher", constants::MICK_SCHUMACHER},
        {"Nicholas Latifi", constants::NICHOLAS_LATIFI},
        {"Antonio Giovinazzi", constants::ANTONIO_GIOVINAZZI},
        {"Kimi Raikkönen", constants::KIMI_RAIKKONEN},
        {"Nikita Mazepin", constants::NIKITA_MAZEPIN},
        {"Robert Kubica", constants::ROBERT_KUBICA}};

template <typename Enum>
Enum do_lookup(
    std::string_view name,
    const std::unordered_map<std::string_view, Enum>& mapping,
    std::string_view error_name) {
  auto itr = mapping.find(name);
  if (itr == mapping.end()) {
    std::cerr << "Unknown " << error_name << " name: " << name << std::endl;
    std::exit(1);
  }
  return itr->second;
}

} // namespace

constants::Circuit lookup_circuit(std::string_view circuit_name) {
  return do_lookup(circuit_name, NAME_TO_CIRCUIT_MAP, "circuit");
}

constants::Driver lookup_driver(std::string_view driver_name) {
  return do_lookup(driver_name, NAME_TO_DRIVER_MAP, "driver");
}

constants::Team lookup_team(std::string_view team_name) {
  return do_lookup(team_name, NAME_TO_TEAM_MAP, "team");
}

} // namespace f1_predict
