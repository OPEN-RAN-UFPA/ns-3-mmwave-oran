/* -*-  Mode: C++; c-file-style: "gnu"; indent-tabs-mode:nil; -*- */
/* *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Authors: Combined work from scenario-one and scenario-three.
 * Original Authors: Andrea Lacava, Michele Polese, Matteo Bordin
 * * Research Implementation: Elioth Luy, working on the research paper of MSc. Kleber Vilhena.
 * Affiliation: Federal University of Pará (UFPA).
 */

/**
 * @file scenario-hierarchical.cc
 * @brief This scenario combines the functionalities of Traffic Steering (TS) and Energy Saving (ES).
 *
 * @details
 * This implementation utilizes "Scenario One" and "Scenario Three" (Traffic Steering and Energy Saving)
 * for the use and control of a Deep Reinforcement Learning (DRL) agent. It employs online training
 * without heuristics, ensuring that the control is performed exclusively by the DRL model.
 *
 * Key features:
 * 1.  Accepts control actions for both forced handovers (TS) and cell ON/OFF state (ES)
 * through a single control file. The ns-3 device is expected to parse actions based on the header.
 * 2.  Generates all necessary KPIs for both TS (per-UE SINR, throughput) and ES (aggregated metrics).
 * 3.  Includes the BsStateTrace function to log the ON/OFF state of cells to a separate file (bsState.txt),
 * which is crucial for the ES agent's observation.
 * 4.  Combines all configurable parameters from both original scenarios.
 */
#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/mobility-module.h"
#include "ns3/applications-module.h"
#include "ns3/point-to-point-helper.h"
#include <ns3/lte-ue-net-device.h>
#include "ns3/mmwave-helper.h"
#include "ns3/epc-helper.h"
#include "ns3/mmwave-point-to-point-epc-helper.h"
#include "ns3/lte-helper.h"
#include "ns3/energy-heuristic.h" // Embora não usemos heurísticas aqui, pode ser necessário para compilar

using namespace ns3;
using namespace mmwave;


NS_LOG_COMPONENT_DEFINE ("ScenarioHierarchical");

// --- Funções de Log (do scenario-three.cc) ---
std::ofstream outFile;
void
BsStateTrace (std::string filename, Ptr<LteEnbNetDevice> ltedev, Ptr<LteEnbRrc> lte_rrc )
{
  if (!outFile.is_open ())
  {
    outFile.open (filename.c_str (), std::ios_base::out | std::ios_base::trunc);
    NS_LOG_LOGIC ("File opened");
    outFile << "Timestamp"
    << " "
    << "UNIX"
    << " "
    << "Id"
    << " "
    << "State" << std::endl;
  }
  // Lê o mapa que indica se handover é permitido para cada célula secundária
  std::map<uint16_t, bool> entry = lte_rrc->GetAllowHandoverTo();
  for (auto it = entry.begin(); it != entry.end(); it++)
  {
    // Calcula o timestamp UNIX em milissegundos
    uint64_t unix_timestamp_ms = ltedev->GetStartTime() + Simulator::Now ().GetMilliSeconds ();
    // Escreve: Tempo Simulação (s), Timestamp UNIX (ms), CellID, Estado (1=Permitido/ON, 0=Não Permitido/OFF)
    outFile << Simulator::Now ().GetSeconds () << " " << unix_timestamp_ms << " "
    << it->first << " " << it->second << std::endl;
  }
}

// Função para imprimir a posição das UEs para gnuplot
void
PrintGnuplottableUeListToFile (std::string filename)
{
  std::ofstream outFile;
  outFile.open (filename.c_str (), std::ios_base::out | std::ios_base::trunc);
  if (!outFile.is_open ())
  {
    NS_LOG_ERROR ("Can't open file " << filename);
    return;
  }
  for (NodeList::Iterator it = NodeList::Begin (); it != NodeList::End (); ++it)
  {
    Ptr<Node> node = *it;
    int nDevs = node->GetNDevices ();
    for (int j = 0; j < nDevs; j++)
    {
      Ptr<McUeNetDevice> mcuedev = node->GetDevice (j)->GetObject<McUeNetDevice> ();
      if (mcuedev)
      {
        Vector pos = node->GetObject<MobilityModel> ()->GetPosition ();
        outFile << "set label \"" << mcuedev->GetImsi () << "\" at " << pos.x << "," << pos.y
        << " left font \"Helvetica,8\" textcolor rgb \"black\" front point pt 1 ps "
        "0.3 lc rgb \"black\" offset 0,0"
        << std::endl;
      }
    }
  }
}

// Função para imprimir a posição das eNBs/gNBs para gnuplot
void
PrintGnuplottableEnbListToFile (std::string filename)
{
  std::ofstream outFile;
  outFile.open (filename.c_str (), std::ios_base::out | std::ios_base::trunc);
  if (!outFile.is_open ())
  {
    NS_LOG_ERROR ("Can't open file " << filename);
    return;
  }
  for (NodeList::Iterator it = NodeList::Begin (); it != NodeList::End (); ++it)
  {
    Ptr<Node> node = *it;
    int nDevs = node->GetNDevices ();
    for (int j = 0; j < nDevs; j++)
    {
      Ptr<LteEnbNetDevice> enbdev = node->GetDevice (j)->GetObject<LteEnbNetDevice> ();
      Ptr<MmWaveEnbNetDevice> mmdev = node->GetDevice (j)->GetObject<MmWaveEnbNetDevice> ();
      if (enbdev) // eNB LTE (azul)
      {
        Vector pos = node->GetObject<MobilityModel> ()->GetPosition ();
        outFile << "set label \"" << enbdev->GetCellId () << "\" at " << pos.x << "," << pos.y
        << " left font \"Helvetica,8\" textcolor rgb \"blue\" front  point pt 4 ps "
        "0.3 lc rgb \"blue\" offset 0,0"
        << std::endl;
      }
      else if (mmdev) // gNB mmWave (vermelho)
      {
        Vector pos = node->GetObject<MobilityModel> ()->GetPosition ();
        outFile << "set label \"" << mmdev->GetCellId () << "\" at " << pos.x << "," << pos.y
        << " left font \"Helvetica,8\" textcolor rgb \"red\" front  point pt 4 ps "
        "0.3 lc rgb \"red\" offset 0,0"
        << std::endl;
      }
    }
  }
}

// --- Parâmetros Globais (mesclados de ambos os cenários) ---

// Parâmetros Comuns
static ns3::GlobalValue g_simTime ("simTime", "Simulation time in seconds", ns3::DoubleValue (10.0), ns3::MakeDoubleChecker<double> (0.1, 1000.0));
static ns3::GlobalValue g_ues ("ues", "Number of UEs for each mmWave ENB.", ns3::UintegerValue (9), ns3::MakeUintegerChecker<uint8_t> ()); // Ajustado para corresponder ao seu log
static ns3::GlobalValue g_indicationPeriodicity ("indicationPeriodicity", "E2 Indication Periodicity reports (value in seconds)", ns3::DoubleValue (0.1), ns3::MakeDoubleChecker<double> (0.01, 2.0));
static ns3::GlobalValue g_configuration ("configuration", "Set the wanted configuration to emulate [0,2]", ns3::UintegerValue (0), ns3::MakeUintegerChecker<uint8_t> ()); // Ajustado para corresponder ao seu log
static ns3::GlobalValue g_trafficModel ("trafficModel", "Type of the traffic model [0,3]", ns3::UintegerValue (3), ns3::MakeUintegerChecker<uint8_t> ()); // Ajustado para corresponder ao seu log
static ns3::GlobalValue q_useSemaphores ("useSemaphores", "If true, enables the use of semaphores for external environment control", ns3::BooleanValue (false), ns3::MakeBooleanChecker ()); // Ajustado para corresponder ao seu log de erro
static ns3::GlobalValue g_controlFileName ("controlFileName", "The path to the control file for hierarchical actions", ns3::StringValue (""), ns3::MakeStringChecker ()); // Definido para o esperado
//hierarchical_actions.csv
// Parâmetros de Handover (do scenario-one)
static ns3::GlobalValue g_hoSinrDifference ("hoSinrDifference", "The SINR value difference for which a handover is triggered", ns3::DoubleValue (3), ns3::MakeDoubleChecker<double> ());

// Parâmetros de Mobilidade (do scenario-three)
static ns3::GlobalValue g_positionAllocator ("positionAllocator", "UE position allocator type [0,1]", ns3::UintegerValue (0), ns3::MakeUintegerChecker<uint8_t> ());
static ns3::GlobalValue g_nBsNoUesAlloc ("nBsNoUesAlloc", "Number of BS without initial UEs allocated", ns3::IntegerValue (-1), ns3::MakeIntegerChecker<int8_t> ());
static ns3::GlobalValue g_minSpeed ("minSpeed", "minimum UE speed in m/s", ns3::DoubleValue (2.0), ns3::MakeDoubleChecker<double> ());
static ns3::GlobalValue g_maxSpeed ("maxSpeed", "maximum UE speed in m/s", ns3::DoubleValue (4.0), ns3::MakeDoubleChecker<double> ());

// Parâmetros Técnicos (comuns ou de um dos cenários)
static ns3::GlobalValue g_bufferSize ("bufferSize", "RLC tx buffer size (MB)", ns3::UintegerValue (1), ns3::MakeUintegerChecker<uint32_t> ());
static ns3::GlobalValue g_rlcAmEnabled ("rlcAmEnabled", "If true, use RLC AM, else use RLC UM", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_enableTraces ("enableTraces", "If true, generate ns-3 traces", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2lteEnabled ("e2lteEnabled", "If true, send LTE E2 reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2nrEnabled ("e2nrEnabled", "If true, send NR E2 reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2du ("e2du", "If true, send DU reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2cuUp ("e2cuUp", "If true, send CU-UP reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2cuCp ("e2cuCp", "If true, send CU-CP reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_dataRate ("dataRate", "Set the data rate to be used [0=low, 1=high]", ns3::DoubleValue (0), ns3::MakeDoubleChecker<double> (0, 1));
static ns3::GlobalValue g_reducedPmValues ("reducedPmValues", "If true, use a subset of the pm containers", ns3::BooleanValue (false), ns3::MakeBooleanChecker ()); // Ajustado para corresponder ao seu log
static ns3::GlobalValue g_outageThreshold ("outageThreshold", "SNR threshold for outage events [dB]", ns3::DoubleValue (-5.0), ns3::MakeDoubleChecker<double> ()); // Ajustado para corresponder ao seu log
static ns3::GlobalValue g_basicCellId ("basicCellId", "The next value will be the first cellId", ns3::UintegerValue (1), ns3::MakeUintegerChecker<uint8_t> ());
static ns3::GlobalValue g_numberOfRaPreambles ("numberOfRaPreambles", "Number of RA preambles", ns3::UintegerValue (40), ns3::MakeUintegerChecker<uint8_t> ()); // Ajustado para corresponder ao seu log
static ns3::GlobalValue g_handoverMode ("handoverMode", "HO euristic to be used", ns3::StringValue ("DynamicTtt"), ns3::MakeStringChecker ()); // Ajustado para corresponder ao seu log
static ns3::GlobalValue g_e2TermIp ("e2TermIp", "The IP address of the RIC E2 termination", ns3::StringValue ("127.0.0.1"), ns3::MakeStringChecker ());
static ns3::GlobalValue g_enableE2FileLogging ("enableE2FileLogging", "If true, generate offline file logging instead of connecting to RIC", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());

// Adicionado RngRun para corresponder ao seu log
static ns3::GlobalValue g_rngRun ("RngRun", "Seed for random number generation", ns3::UintegerValue (555), ns3::MakeUintegerChecker<uint32_t> ());

int
main (int argc, char *argv[])
{
  // Configura o nível de log
  // LogComponentEnableAll (LOG_PREFIX_ALL); // Descomente para log máximo
  LogComponentEnable ("ScenarioHierarchical", LOG_LEVEL_INFO);
  // LogComponentEnable ("LteEnbNetDevice", LOG_LEVEL_DEBUG); // Descomente para depurar leitura do ficheiro
  // LogComponentEnable ("LteEnbRrc", LOG_LEVEL_DEBUG); // Descomente para depurar HO e estado

  // Define os limites do cenário
  double maxXAxis = 4000;
  double maxYAxis = 4000;

  // Processa argumentos da linha de comando
  CommandLine cmd;
  cmd.Parse (argc, argv);

  // --- Leitura e Configuração de Parâmetros ---
  bool harqEnabled = true;
  UintegerValue uintegerValue;
  IntegerValue integerValue;
  BooleanValue booleanValue;
  StringValue stringValue;
  DoubleValue doubleValue;

  // Lê os valores dos parâmetros globais para variáveis locais
  GlobalValue::GetValueByName ("hoSinrDifference", doubleValue);
  double hoSinrDifference = doubleValue.Get ();
  GlobalValue::GetValueByName ("dataRate", doubleValue);
  double dataRateFromConf = doubleValue.Get ();
  GlobalValue::GetValueByName ("rlcAmEnabled", booleanValue);
  bool rlcAmEnabled = booleanValue.Get ();
  GlobalValue::GetValueByName ("bufferSize", uintegerValue);
  uint32_t bufferSize = uintegerValue.Get ();
  GlobalValue::GetValueByName ("basicCellId", uintegerValue);
  uint16_t basicCellId = uintegerValue.Get ();
  (void) basicCellId; // Evita aviso de não utilizado
  GlobalValue::GetValueByName ("enableTraces", booleanValue);
  bool enableTraces = booleanValue.Get ();
  GlobalValue::GetValueByName ("trafficModel", uintegerValue);
  uint8_t trafficModel = uintegerValue.Get ();
  (void) trafficModel; // Evita aviso de não utilizado
  GlobalValue::GetValueByName ("nBsNoUesAlloc", integerValue);
  int8_t nBsNoUesAlloc = integerValue.Get ();
  (void) nBsNoUesAlloc; // Evita aviso de não utilizado
  GlobalValue::GetValueByName ("positionAllocator", uintegerValue);
  uint8_t positionAllocator = uintegerValue.Get ();
  GlobalValue::GetValueByName ("outageThreshold",doubleValue);
  double outageThreshold = doubleValue.Get ();
  GlobalValue::GetValueByName ("handoverMode", stringValue);
  std::string handoverMode = stringValue.Get ();
  GlobalValue::GetValueByName ("minSpeed", doubleValue);
  double minSpeed = doubleValue.Get ();
  GlobalValue::GetValueByName ("maxSpeed", doubleValue);
  double maxSpeed = doubleValue.Get ();
  GlobalValue::GetValueByName ("indicationPeriodicity", doubleValue);
  double indicationPeriodicity = doubleValue.Get ();
  GlobalValue::GetValueByName ("useSemaphores", booleanValue);
  bool useSemaphores = booleanValue.Get ();
  GlobalValue::GetValueByName ("controlFileName", stringValue);
  std::string controlFilename = stringValue.Get ();

  // E2 Logging settings
  GlobalValue::GetValueByName ("e2lteEnabled", booleanValue);
  bool e2lteEnabled = booleanValue.Get ();
  GlobalValue::GetValueByName ("e2nrEnabled", booleanValue);
  bool e2nrEnabled = booleanValue.Get ();
  GlobalValue::GetValueByName ("e2du", booleanValue);
  bool e2du = booleanValue.Get ();
  GlobalValue::GetValueByName ("e2cuUp", booleanValue);
  bool e2cuUp = booleanValue.Get ();
  GlobalValue::GetValueByName ("e2cuCp", booleanValue);
  bool e2cuCp = booleanValue.Get ();
  GlobalValue::GetValueByName ("reducedPmValues", booleanValue);
  bool reducedPmValues = booleanValue.Get ();
  GlobalValue::GetValueByName ("enableE2FileLogging", booleanValue);
  bool enableE2FileLogging = booleanValue.Get ();

  // --- CORREÇÃO: Adiciona leituras em falta ---
  GlobalValue::GetValueByName ("numberOfRaPreambles", uintegerValue);
  uint8_t numberOfRaPreambles = uintegerValue.Get ();
  GlobalValue::GetValueByName ("e2TermIp", stringValue);
  std::string e2TermIp = stringValue.Get ();
  // --- FIM DA CORREÇÃO ---

  // Aplica a seed RngRun
  GlobalValue::GetValueByName("RngRun", uintegerValue);
  RngSeedManager::SetRun (uintegerValue.Get ());


  // --- Configurações Padrão do ns-3 (mescladas e corrigidas) ---
  Config::SetDefault ("ns3::LteEnbNetDevice::UseSemaphores", BooleanValue (useSemaphores));
  Config::SetDefault ("ns3::LteEnbNetDevice::ControlFileName", StringValue(controlFilename));
  Config::SetDefault ("ns3::LteEnbNetDevice::E2Periodicity", DoubleValue (indicationPeriodicity));
  Config::SetDefault ("ns3::MmWaveEnbNetDevice::E2Periodicity", DoubleValue (indicationPeriodicity));

  // Configuração E2
  Config::SetDefault ("ns3::MmWaveHelper::E2ModeLte", BooleanValue(e2lteEnabled));
  Config::SetDefault ("ns3::MmWaveHelper::E2ModeNr", BooleanValue(e2nrEnabled));
  Config::SetDefault ("ns3::MmWaveHelper::E2Periodicity", DoubleValue (indicationPeriodicity));
  Config::SetDefault ("ns3::MmWaveEnbNetDevice::EnableDuReport", BooleanValue(e2du));
  Config::SetDefault ("ns3::MmWaveEnbNetDevice::EnableCuUpReport", BooleanValue(e2cuUp));
  Config::SetDefault ("ns3::LteEnbNetDevice::EnableCuUpReport", BooleanValue(e2cuUp));
  Config::SetDefault ("ns3::MmWaveEnbNetDevice::EnableCuCpReport", BooleanValue(e2cuCp));
  Config::SetDefault ("ns3::LteEnbNetDevice::EnableCuCpReport", BooleanValue(e2cuCp));
  Config::SetDefault ("ns3::MmWaveEnbNetDevice::ReducedPmValues", BooleanValue (reducedPmValues));
  Config::SetDefault ("ns3::LteEnbNetDevice::ReducedPmValues", BooleanValue (reducedPmValues));
  Config::SetDefault ("ns3::LteEnbNetDevice::EnableE2FileLogging", BooleanValue (enableE2FileLogging));
  Config::SetDefault ("ns3::MmWaveEnbNetDevice::EnableE2FileLogging", BooleanValue (enableE2FileLogging));

  // Configuração RRC e Handover
  Config::SetDefault ("ns3::LteEnbRrc::OutageThreshold", DoubleValue (outageThreshold));
  Config::SetDefault ("ns3::LteEnbRrc::SecondaryCellHandoverMode", StringValue (handoverMode));
  Config::SetDefault ("ns3::LteEnbRrc::HoSinrDifference", DoubleValue (hoSinrDifference));

  // Outras configurações (incluindo correções HARQ e RACH)
  Config::SetDefault ("ns3::MmWaveHelper::RlcAmEnabled", BooleanValue (rlcAmEnabled));
  Config::SetDefault ("ns3::MmWaveHelper::HarqEnabled", BooleanValue (harqEnabled));
  Config::SetDefault ("ns3::MmWaveFlexTtiMacScheduler::HarqEnabled", BooleanValue (harqEnabled)); // Garante consistência
  Config::SetDefault ("ns3::MmWavePhyMacCommon::NumHarqProcess", UintegerValue (100)); // --- CORREÇÃO HARQ ---
  Config::SetDefault ("ns3::MmWaveEnbMac::NumberOfRaPreambles", UintegerValue (numberOfRaPreambles)); // Usa variável lida
  Config::SetDefault ("ns3::MmWaveHelper::UseIdealRrc", BooleanValue (true)); // Comum nos outros
  Config::SetDefault ("ns3::MmWaveHelper::BasicCellId", UintegerValue (basicCellId)); // Usa variável lida
  Config::SetDefault ("ns3::MmWaveHelper::BasicImsi", UintegerValue ((basicCellId-1))); // Usa variável lida
  Config::SetDefault ("ns3::MmWaveHelper::E2TermIp", StringValue (e2TermIp)); // Usa variável lida
  Config::SetDefault ("ns3::ThreeGppChannelModel::UpdatePeriod", TimeValue (MilliSeconds (100.0))); // Comum nos outros
  Config::SetDefault ("ns3::ThreeGppChannelConditionModel::UpdatePeriod", TimeValue (MilliSeconds (100))); // Comum nos outros
  Config::SetDefault ("ns3::LteRlcAm::ReportBufferStatusTimer", TimeValue (MilliSeconds (10.0))); // Comum nos outros
  Config::SetDefault ("ns3::LteRlcUmLowLat::ReportBufferStatusTimer", TimeValue (MilliSeconds (10.0))); // Comum nos outros
  Config::SetDefault ("ns3::LteRlcUm::MaxTxBufferSize", UintegerValue (bufferSize * 1024 * 1024));
  Config::SetDefault ("ns3::LteRlcUmLowLat::MaxTxBufferSize", UintegerValue (bufferSize * 1024 * 1024));
  Config::SetDefault ("ns3::LteRlcAm::MaxTxBufferSize", UintegerValue (bufferSize * 1024 * 1024));


  // --- Construção do Cenário (baseado no scenario-three) ---

  // Configuração de Frequência, Largura de Banda, ISD, Antenas e Data Rate
  double bandwidth;
  double centerFrequency;
  double isd;
  int numAntennasMcUe;
  (void) numAntennasMcUe; // Evita aviso
  int numAntennasMmWave;
  (void) numAntennasMmWave; // Evita aviso
  std::string dataRate;

  GlobalValue::GetValueByName ("configuration", uintegerValue);
  uint8_t configuration = uintegerValue.Get ();
  switch (configuration)
  {
    case 0:
      centerFrequency = 850e6; bandwidth = 20e6; isd = 1700;
      numAntennasMcUe = 1; numAntennasMmWave = 1;
      dataRate = (dataRateFromConf == 0 ? "1.5Mbps" : "4.5Mbps");
      break;
    case 1:
      centerFrequency = 3.5e9; bandwidth = 20e6; isd = 1000;
      numAntennasMcUe = 1; numAntennasMmWave = 1;
      dataRate = (dataRateFromConf == 0 ? "1.5Mbps" : "4.5Mbps");
      break;
    case 2:
      centerFrequency = 28e9; bandwidth = 100e6; isd = 200;
      numAntennasMcUe = 16; numAntennasMmWave = 64;
      dataRate = (dataRateFromConf == 0 ? "15Mbps" : "45Mbps");
      break;
    default:
      NS_FATAL_ERROR ("Configuration not recognized" << configuration);
      break;
  }

  // Aplica configurações de BW e Frequência Central
  Config::SetDefault ("ns3::MmWavePhyMacCommon::Bandwidth", DoubleValue (bandwidth));
  Config::SetDefault ("ns3::MmWavePhyMacCommon::CenterFreq", DoubleValue (centerFrequency));

  // Cria Helpers
  Ptr<MmWaveHelper> mmwaveHelper = CreateObject<MmWaveHelper> ();
  mmwaveHelper->SetPathlossModelType ("ns3::ThreeGppUmiStreetCanyonPropagationLossModel");
  // Adiciona configuração de ChannelConditionModel (presente no scenario-three)
  mmwaveHelper->SetChannelConditionModelType ("ns3::ThreeGppUmiStreetCanyonChannelConditionModel");

  // Configura Antenas (presente no scenario-three)
  mmwaveHelper->SetUePhasedArrayModelAttribute("NumColumns", UintegerValue(std::sqrt(numAntennasMcUe)));
  mmwaveHelper->SetUePhasedArrayModelAttribute("NumRows", UintegerValue(std::sqrt(numAntennasMcUe)));
  mmwaveHelper->SetEnbPhasedArrayModelAttribute("NumColumns",UintegerValue(std::sqrt(numAntennasMmWave)));
  mmwaveHelper->SetEnbPhasedArrayModelAttribute("NumRows", UintegerValue(std::sqrt(numAntennasMmWave)));

  Ptr<MmWavePointToPointEpcHelper> epcHelper = CreateObject<MmWavePointToPointEpcHelper> ();
  mmwaveHelper->SetEpcHelper (epcHelper);

  // Define número de nós
  uint8_t nMmWaveEnbNodes = 7;
  uint8_t nLteEnbNodes = 1;
  GlobalValue::GetValueByName ("ues", uintegerValue);
  uint32_t ues_per_gnb = uintegerValue.Get (); // Renomeado para clareza
  uint8_t nUeNodes = ues_per_gnb * nMmWaveEnbNodes;

  // Cria nós EPC (PGW) e Host Remoto
  Ptr<Node> pgw = epcHelper->GetPgwNode ();
  NodeContainer remoteHostContainer;
  remoteHostContainer.Create (1);
  Ptr<Node> remoteHost = remoteHostContainer.Get (0);
  InternetStackHelper internet;
  internet.Install (remoteHostContainer);

  // Conecta Host Remoto ao PGW
  PointToPointHelper p2ph;
  p2ph.SetDeviceAttribute ("DataRate", DataRateValue (DataRate ("100Gb/s")));
  p2ph.SetDeviceAttribute ("Mtu", UintegerValue (2500));
  // Adiciona Delay (presente no scenario-three)
  p2ph.SetChannelAttribute ("Delay", TimeValue (Seconds (0.010)));
  NetDeviceContainer internetDevices = p2ph.Install (pgw, remoteHost);
  Ipv4AddressHelper ipv4h;
  ipv4h.SetBase ("1.0.0.0", "255.0.0.0");
  Ipv4InterfaceContainer internetIpIfaces = ipv4h.Assign (internetDevices);
  Ipv4Address remoteHostAddr = internetIpIfaces.GetAddress (1);
  (void) remoteHostAddr; // Evita aviso
  Ipv4StaticRoutingHelper ipv4RoutingHelper;
  Ptr<Ipv4StaticRouting> remoteHostStaticRouting = ipv4RoutingHelper.GetStaticRouting (remoteHost->GetObject<Ipv4> ());
  remoteHostStaticRouting->AddNetworkRouteTo (Ipv4Address ("7.0.0.0"), Ipv4Mask ("255.0.0.0"), 1);

  // Cria nós UE e eNB/gNB
  NodeContainer ueNodes;
  NodeContainer mmWaveEnbNodes;
  NodeContainer lteEnbNodes;
  NodeContainer allEnbNodes;
  mmWaveEnbNodes.Create (nMmWaveEnbNodes);
  lteEnbNodes.Create (nLteEnbNodes);
  ueNodes.Create (nUeNodes);
  allEnbNodes.Add (lteEnbNodes);
  allEnbNodes.Add (mmWaveEnbNodes);

  // Posiciona eNBs/gNBs (Layout Hexagonal)
  Vector centerPosition = Vector (maxXAxis / 2, maxYAxis / 2, 3);
  Ptr<ListPositionAllocator> enbPositionAlloc = CreateObject<ListPositionAllocator> ();
  enbPositionAlloc->Add (centerPosition); // LTE eNB (CellID 1, provavelmente)
  enbPositionAlloc->Add (centerPosition); // Co-located mmWave gNB (CellID 2, provavelmente)

  for (int8_t i = 0; i < (nMmWaveEnbNodes - 1); ++i) // Posiciona os 6 gNBs restantes
  {
    double x = isd * cos ((2 * M_PI * i) / (nMmWaveEnbNodes - 1));
    double y = isd * sin ((2 * M_PI * i) / (nMmWaveEnbNodes - 1));
    enbPositionAlloc->Add (Vector (centerPosition.x + x, centerPosition.y + y, 3));
  }

  MobilityHelper enbmobility;
  enbmobility.SetMobilityModel ("ns3::ConstantPositionMobilityModel");
  enbmobility.SetPositionAllocator (enbPositionAlloc);
  enbmobility.Install (allEnbNodes);

  // Posiciona UEs (Lógica de mobilidade flexível do scenario-three)
  MobilityHelper uemobility;
  Ptr<UniformRandomVariable> speedVar = CreateObject<UniformRandomVariable> (); // Renomeado para evitar conflito
  speedVar->SetAttribute ("Min", DoubleValue (minSpeed));
  speedVar->SetAttribute ("Max", DoubleValue (maxSpeed));
  // Adiciona Random Variable para tempo de mudança de direção (do scenario-three)
  Ptr<UniformRandomVariable> puntTimeDirection = CreateObject<UniformRandomVariable> ();
  puntTimeDirection->SetAttribute ("Min", DoubleValue (1));
  puntTimeDirection->SetAttribute ("Max", DoubleValue (3));
  double timeDirection=puntTimeDirection->GetValue();


  switch (positionAllocator)
  {
    case 0: { // Distribuição uniforme no disco central
      Ptr<UniformDiscPositionAllocator> uePositionAlloc = CreateObject<UniformDiscPositionAllocator> ();
      uePositionAlloc->SetX(centerPosition.x);
      uePositionAlloc->SetY(centerPosition.y);
      uePositionAlloc->SetRho(isd);
      // Usa RandomWalk2dMobilityModel com tempo (do scenario-three)
      uemobility.SetMobilityModel("ns3::RandomWalk2dMobilityModel",
                                  "Mode", StringValue("Time"),
                                  "Time", StringValue(std::to_string(timeDirection) + "s"),
                                  "Speed", PointerValue(speedVar),
                                  "Bounds", RectangleValue(Rectangle(0, maxXAxis, 0, maxYAxis)));
      uemobility.SetPositionAllocator(uePositionAlloc);
      uemobility.Install(ueNodes);
      break;
    }
    case 1: { // Alocação em torno de um subconjunto de BSs (lógica complexa do scenario-three)
      if (nBsNoUesAlloc == -1)
      {
        NS_FATAL_ERROR("nBsNoUesAlloc (-1) incorrecto para positionAllocator=1.");
      }
      if (nBsNoUesAlloc >= nMmWaveEnbNodes)
      {
        NS_FATAL_ERROR("nBsNoUesAlloc (" << nBsNoUesAlloc << ") maior ou igual ao número de gNBs mmWave (" << nMmWaveEnbNodes << ").");
      }

      // Copia posições dos gNBs mmWave para um array
      std::vector<Vector> bsCoords;
      // Pula a posição do eNB LTE e do gNB co-localizado
      // NOTA: Assume que enbPositionAlloc->GetNext() retorna na ordem: LTE, gNB central, gNBs periféricos
      Ptr<ListPositionAllocator> tempAlloc = CreateObject<ListPositionAllocator>(*enbPositionAlloc); // Cria cópia para não alterar o original
      tempAlloc->GetNext(); // Pula LTE eNB
      for (int i = 0; i < nMmWaveEnbNodes; i++)
      {
        bsCoords.push_back(tempAlloc->GetNext());
      }

      // Embaralha as posições para escolher aleatoriamente quais BSs não terão UEs
      std::srand(std::time(0)); // Usa time(0) para seed
      std::random_shuffle(bsCoords.begin(), bsCoords.end());

      NS_LOG_INFO("Alocando UEs em torno de " << (nMmWaveEnbNodes - nBsNoUesAlloc) << " gNBs mmWave.");

      // Calcula quantos UEs por gNB ativo
      uint32_t numActiveGnbs = nMmWaveEnbNodes - nBsNoUesAlloc;
      uint32_t nodeGroupSize = nUeNodes / numActiveGnbs;
      uint32_t nodeGroupSizeRest = nUeNodes % numActiveGnbs;
      uint32_t ueIndexCounter = 0;

      // Aloca UEs nos gNBs selecionados
      for (uint32_t bsCoordIndex = 0; bsCoordIndex < numActiveGnbs; bsCoordIndex++)
      {
        Ptr<UniformDiscPositionAllocator> uePositionAlloc = CreateObject<UniformDiscPositionAllocator> ();
        uePositionAlloc->SetX(bsCoords[bsCoordIndex].x);
        uePositionAlloc->SetY(bsCoords[bsCoordIndex].y);
        uePositionAlloc->SetRho(isd / 2); // Raio menor em torno da BS específica
        uemobility.SetMobilityModel("ns3::RandomWalk2dMobilityModel",
                                    "Mode", StringValue("Time"),
                                    "Time", StringValue(std::to_string(timeDirection) + "s"),
                                    "Speed", PointerValue(speedVar),
                                    "Bounds", RectangleValue(Rectangle(0, maxXAxis, 0, maxYAxis)));
        uemobility.SetPositionAllocator(uePositionAlloc);

        uint32_t uesInThisGroup = nodeGroupSize + (bsCoordIndex < nodeGroupSizeRest ? 1 : 0);
        NodeContainer currentUeGroup;
        for(uint32_t i=0; i < uesInThisGroup && ueIndexCounter < nUeNodes; ++i)
        {
          currentUeGroup.Add(ueNodes.Get(ueIndexCounter++));
        }
        if (currentUeGroup.GetN() > 0)
        {
          uemobility.Install(currentUeGroup);
          NS_LOG_INFO("Instalado " << currentUeGroup.GetN() << " UEs em torno da BS na posição " << bsCoords[bsCoordIndex]);
        }
      }
      if (ueIndexCounter != nUeNodes) {
        NS_LOG_WARN("Nem todos os UEs foram alocados em positionAllocator=1. UEs alocados: " << ueIndexCounter << ", Total UEs: " << nUeNodes);
      }
      break;
    }
    default:
      NS_FATAL_ERROR("positionAllocator not recognized " << positionAllocator);
      break;
  }

  // Instala NetDevices LTE, mmWave e MC (Multi-Connectivity)
  NetDeviceContainer lteEnbDevs = mmwaveHelper->InstallLteEnbDevice (lteEnbNodes);
  NetDeviceContainer mmWaveEnbDevs = mmwaveHelper->InstallEnbDevice (mmWaveEnbNodes);
  NetDeviceContainer mcUeDevs = mmwaveHelper->InstallMcUeDevice (ueNodes);

  // Instala stack IP nas UEs e atribui endereços
  internet.Install (ueNodes);
  Ipv4InterfaceContainer ueIpIface = epcHelper->AssignUeIpv4Address (NetDeviceContainer (mcUeDevs));

  // Configura rotas default para as UEs
  for (uint32_t u = 0; u < ueNodes.GetN (); ++u)
  {
    Ptr<Node> ueNode = ueNodes.Get (u);
    Ptr<Ipv4StaticRouting> ueStaticRouting = ipv4RoutingHelper.GetStaticRouting (ueNode->GetObject<Ipv4> ());
    ueStaticRouting->SetDefaultRoute (epcHelper->GetUeDefaultGatewayAddress (), 1);
  }

  // Adiciona interface X2 entre eNBs/gNBs
  mmwaveHelper->AddX2Interface (lteEnbNodes, mmWaveEnbNodes);

  // Associa UEs à eNB/gNB mais próxima inicialmente
  mmwaveHelper->AttachToClosestEnb (mcUeDevs, mmWaveEnbDevs, lteEnbDevs);

  // --- Setup das Aplicações (tráfego - lógica do scenario-three) ---
  uint16_t portTcp = 50000;
  Address sinkLocalAddressTcp (InetSocketAddress (Ipv4Address::GetAny (), portTcp));
  PacketSinkHelper sinkHelperTcp ("ns3::TcpSocketFactory", sinkLocalAddressTcp);
  AddressValue serverAddressTcp (InetSocketAddress (remoteHostAddr, portTcp));

  uint16_t portUdp = 60000;
  Address sinkLocalAddressUdp (InetSocketAddress (Ipv4Address::GetAny (), portUdp));
  PacketSinkHelper sinkHelperUdp ("ns3::UdpSocketFactory", sinkLocalAddressUdp);
  AddressValue serverAddressUdp (InetSocketAddress (remoteHostAddr, portUdp));

  ApplicationContainer sinkApp;
  sinkApp.Add (sinkHelperTcp.Install (remoteHost)); // Sink TCP no host remoto
  sinkApp.Add (sinkHelperUdp.Install (remoteHost)); // Sink UDP no host remoto

  // Cria helpers para clientes OnOff
  OnOffHelper clientHelperTcp ("ns3::TcpSocketFactory", Address ());
  clientHelperTcp.SetAttribute ("Remote", serverAddressTcp);
  clientHelperTcp.SetAttribute ("OnTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]")); // Média 1s ON
  clientHelperTcp.SetAttribute ("OffTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]")); // Média 1s OFF
  clientHelperTcp.SetAttribute ("DataRate", StringValue (dataRate)); // Usa dataRate configurado
  clientHelperTcp.SetAttribute ("PacketSize", UintegerValue (1280));

  OnOffHelper clientHelperTcp150 ("ns3::TcpSocketFactory", Address ());
  clientHelperTcp150.SetAttribute ("Remote", serverAddressTcp);
  clientHelperTcp150.SetAttribute ("OnTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
  clientHelperTcp150.SetAttribute ("OffTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
  clientHelperTcp150.SetAttribute ("DataRate", StringValue ("150kbps")); // Baixa taxa
  clientHelperTcp150.SetAttribute ("PacketSize", UintegerValue (1280));

  OnOffHelper clientHelperTcp750 ("ns3::TcpSocketFactory", Address ());
  clientHelperTcp750.SetAttribute ("Remote", serverAddressTcp);
  clientHelperTcp750.SetAttribute ("OnTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
  clientHelperTcp750.SetAttribute ("OffTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
  clientHelperTcp750.SetAttribute ("DataRate", StringValue ("750kbps")); // Taxa média
  clientHelperTcp750.SetAttribute ("PacketSize", UintegerValue (1280));

  OnOffHelper clientHelperUdp ("ns3::UdpSocketFactory", Address ());
  clientHelperUdp.SetAttribute ("Remote", serverAddressUdp);
  clientHelperUdp.SetAttribute ("OnTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
  clientHelperUdp.SetAttribute ("OffTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
  clientHelperUdp.SetAttribute ("DataRate", StringValue (dataRate));
  clientHelperUdp.SetAttribute ("PacketSize", UintegerValue (1280));

  ApplicationContainer clientApp;
  switch (trafficModel)
  {
    case 0: { // Full Buffer (constante UDP)
      for (uint32_t u = 0; u < ueNodes.GetN (); ++u)
      {
        PacketSinkHelper dlPacketSinkHelper ("ns3::UdpSocketFactory", InetSocketAddress (Ipv4Address::GetAny (), 1234));
        sinkApp.Add (dlPacketSinkHelper.Install (ueNodes.Get (u))); // Sink na UE
        UdpClientHelper dlClient (ueIpIface.GetAddress (u), 1234); // Cliente no Host Remoto
        dlClient.SetAttribute ("MaxPackets", UintegerValue (UINT32_MAX));
        dlClient.SetAttribute ("PacketSize", UintegerValue (1280));
        // Calcula intervalo para dataRate desejado (aproximado)
        DataRate targetRate(dataRate);
        Time pktInterval = Seconds(1280.0 * 8.0 / targetRate.GetBitRate());
        dlClient.SetAttribute ("Interval", TimeValue (pktInterval));
        clientApp.Add (dlClient.Install (remoteHost));
      }
    }
    break;

    case 1: { // Metade Full Buffer, Metade Bursty (OnOff)
      for (uint32_t u = 0; u < ueNodes.GetN (); ++u)
      {
        if (u % 2 == 0) // Bursty
        {
          PacketSinkHelper dlPacketSinkHelper ("ns3::UdpSocketFactory", InetSocketAddress (Ipv4Address::GetAny (), 1234));
          sinkApp.Add (dlPacketSinkHelper.Install (ueNodes.Get (u))); // Sink na UE
          clientApp.Add (clientHelperUdp.Install (ueNodes.Get(u))); // Cliente OnOff UDP na UE
        }
        else // Full Buffer
        {
          PacketSinkHelper dlPacketSinkHelper ("ns3::UdpSocketFactory", InetSocketAddress (Ipv4Address::GetAny (), 1234));
          sinkApp.Add (dlPacketSinkHelper.Install (ueNodes.Get (u))); // Sink na UE
          UdpClientHelper dlClient (ueIpIface.GetAddress (u), 1234); // Cliente no Host Remoto
          dlClient.SetAttribute ("MaxPackets", UintegerValue (UINT32_MAX));
          dlClient.SetAttribute ("PacketSize", UintegerValue (1280));
          DataRate targetRate(dataRate);
          Time pktInterval = Seconds(1280.0 * 8.0 / targetRate.GetBitRate());
          dlClient.SetAttribute ("Interval", TimeValue (pktInterval));
          clientApp.Add (dlClient.Install (remoteHost));
        }
      }
    }
    break;

    case 2: { // Tudo Bursty (OnOff)
      for (uint32_t u = 0; u < ueNodes.GetN (); ++u)
      {
        // Instala Sink TCP e UDP na UE
        PacketSinkHelper dlPacketSinkTcp ("ns3::TcpSocketFactory", InetSocketAddress (Ipv4Address::GetAny (), 1235));
        PacketSinkHelper dlPacketSinkUdp ("ns3::UdpSocketFactory", InetSocketAddress (Ipv4Address::GetAny (), 1234));
        sinkApp.Add(dlPacketSinkTcp.Install(ueNodes.Get(u)));
        sinkApp.Add(dlPacketSinkUdp.Install(ueNodes.Get(u)));

        // Alterna entre Cliente TCP e UDP na UE
        if (u % 2 == 0)
        {
          // Configura cliente TCP para enviar para o sink TCP na UE
          clientHelperTcp.SetAttribute("Remote", AddressValue(InetSocketAddress(ueIpIface.GetAddress(u), 1235)));
          clientApp.Add (clientHelperTcp.Install (ueNodes.Get (u)));
        }
        else
        {
          // Configura cliente UDP para enviar para o sink UDP na UE
          clientHelperUdp.SetAttribute("Remote", AddressValue(InetSocketAddress(ueIpIface.GetAddress(u), 1234)));
          clientApp.Add (clientHelperUdp.Install (ueNodes.Get (u)));
        }
      }
    }
    break;

    case 3: {
      // Text: "mixture of four heterogeneous traffic models"
      for (uint32_t u = 0; u < ueNodes.GetN (); ++u)
      {
        // Instala Sinks (Receptores) na UE para TCP e UDP
        PacketSinkHelper dlPacketSinkTcp ("ns3::TcpSocketFactory", InetSocketAddress (Ipv4Address::GetAny (), 1235));
        sinkApp.Add(dlPacketSinkTcp.Install(ueNodes.Get(u)));
        PacketSinkHelper dlPacketSinkUdp ("ns3::UdpSocketFactory", InetSocketAddress (Ipv4Address::GetAny (), 1234));
        sinkApp.Add (dlPacketSinkUdp.Install (ueNodes.Get (u)));

        // Destination Address (UE)
        AddressValue ueSinkAddrTcp(InetSocketAddress(ueIpIface.GetAddress(u), 1235));
        AddressValue ueSinkAddrUdp(InetSocketAddress(ueIpIface.GetAddress(u), 1234));

        if (u % 4 == 0) // 25%: TCP full-buffer, 20 Mbps
        {
          // Note: Text says "TCP full-buffer... with a data rate of 20 Mbps".
          // We use OnOff with TCP, High OnTime, and specific DataRate to limit to 20Mbps.
          OnOffHelper client = clientHelperTcp;
          client.SetAttribute("Remote", ueSinkAddrTcp);
          client.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=100.0]")); // Always ON
          client.SetAttribute("OffTime", StringValue("ns3::ConstantRandomVariable[Constant=0.0]"));
          client.SetAttribute("DataRate", StringValue("20Mbps"));
          clientApp.Add(client.Install(remoteHost));
        }
        else if (u % 4 == 1) // 25%: UDP bursty, Avg 20 Mbps
        {
          // Text: "UDP bursty... averaging around 20 Mbps"
          // Assuming Exp(1.0) for ON and OFF, Duty Cycle is 50%.
          // Peak rate must be 40Mbps to average 20Mbps.
          OnOffHelper client = clientHelperUdp;
          client.SetAttribute("Remote", ueSinkAddrUdp);
          client.SetAttribute("OnTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
          client.SetAttribute("OffTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
          client.SetAttribute("DataRate", StringValue("40Mbps"));
          clientApp.Add(client.Install(remoteHost));
        }
        else if (u % 4 == 2) // 25%: TCP bursty, Avg 750 kbps
        {
          // Text: "TCP bursty... averaging 750 kb/s"
          // Peak rate = 1.5 Mbps (50% duty cycle)
          OnOffHelper client = clientHelperTcp;
          client.SetAttribute("Remote", ueSinkAddrTcp);
          client.SetAttribute("OnTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
          client.SetAttribute("OffTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
          client.SetAttribute("DataRate", StringValue("1.5Mbps"));
          clientApp.Add(client.Install(remoteHost));
        }
        else if (u % 4 == 3) // 25%: TCP bursty, Avg 150 kbps
        {
          // Text: "TCP bursty... averaging 150 kbps"
          // Peak rate = 300 kbps (50% duty cycle)
          OnOffHelper client = clientHelperTcp;
          client.SetAttribute("Remote", ueSinkAddrTcp);
          client.SetAttribute("OnTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
          client.SetAttribute("OffTime", StringValue ("ns3::ExponentialRandomVariable[Mean=1.0]"));
          client.SetAttribute("DataRate", StringValue("300kbps"));
          clientApp.Add(client.Install(remoteHost));
        }
      }
      break;
    }

    default:
      NS_FATAL_ERROR ( "Modelo de tráfego inválido: " << trafficModel);
  }


  // --- Início e Fim da Simulação ---
  GlobalValue::GetValueByName ("simTime", doubleValue);
  double simTime = doubleValue.Get ();
  sinkApp.Start (Seconds (0.1)); // Pequeno atraso para garantir que os sinks estão prontos
  clientApp.Start (Seconds (0.2)); // Pequeno atraso para iniciar clientes
  clientApp.Stop (Seconds (simTime - 0.1)); // Para um pouco antes do fim

  // Ativa traces se necessário
  if (enableTraces)
  {
    mmwaveHelper->EnableTraces ();
  }

  // Ativa traces LTE PHY/MAC
  Ptr<LteHelper> lteHelper = CreateObject<LteHelper> ();
  lteHelper->Initialize ();
  lteHelper->EnablePhyTraces ();
  lteHelper->EnableMacTraces ();

  // Imprime posições iniciais para gnuplot
  PrintGnuplottableUeListToFile ("ues.txt");
  PrintGnuplottableEnbListToFile ("enbs.txt");

  // --- Agendamento do Log de Estado da BS (BsStateTrace) ---
  // Obtém o NetDevice e RRC do eNB LTE (assumindo que só há um)
  Ptr<LteEnbNetDevice> ltedev = DynamicCast<LteEnbNetDevice> (lteEnbDevs.Get (0));
  if (!ltedev) {
    NS_FATAL_ERROR("Não foi possível encontrar o LteEnbNetDevice.");
  }
  Ptr<LteEnbRrc> lte_rrc = ltedev->GetRrc ();
  if (!lte_rrc) {
    NS_FATAL_ERROR("Não foi possível obter o LteEnbRrc.");
  }
  // Agenda a escrita do estado a cada `indicationPeriodicity`
  int numSteps = static_cast<int>(std::ceil(simTime / indicationPeriodicity));
  for (int step = 0; step <= numSteps; ++step) {
    double time = step * indicationPeriodicity;
    if (time <= simTime + 0.0001) {
        Simulator::Schedule(Seconds(time), BsStateTrace, "bsState.txt", ltedev, lte_rrc);
    }
}

  // Mensagem de início e execução da simulação
  NS_LOG_UNCOND ("Hierarchical Simulation Starting. Time: " << simTime << " seconds. Control File: '" << controlFilename << "' Use Semaphores: " << useSemaphores);
  Simulator::Stop (Seconds (simTime));
  Simulator::Run ();
  Simulator::Destroy ();
  NS_LOG_INFO ("Done.");
  return 0;
}

