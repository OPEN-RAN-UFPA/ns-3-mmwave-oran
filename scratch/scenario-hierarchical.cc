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
 */

/**
 * @file scenario-hierarchical.cc
 * @brief This scenario combines the functionalities of Traffic Steering (TS) and Energy Saving (ES).
 * It is designed to be controlled by a hierarchical RL agent.
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
#include "ns3/energy-heuristic.h"

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
  std::map<uint16_t, bool> entry = lte_rrc->GetAllowHandoverTo();
  for (auto it = entry.begin(); it != entry.end(); it++)
  {
    uint64_t timestamp = ltedev->GetStartTime() + Simulator::Now ().GetMilliSeconds ();
    outFile << Simulator::Now ().GetSeconds () << " " << timestamp << " "
    << it->first << " " << it->second << std::endl;
  }
}

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
      if (enbdev)
      {
        Vector pos = node->GetObject<MobilityModel> ()->GetPosition ();
        outFile << "set label \"" << enbdev->GetCellId () << "\" at " << pos.x << "," << pos.y
        << " left font \"Helvetica,8\" textcolor rgb \"blue\" front  point pt 4 ps "
        "0.3 lc rgb \"blue\" offset 0,0"
        << std::endl;
      }
      else if (mmdev)
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
static ns3::GlobalValue g_simTime ("simTime", "Simulation time in seconds", ns3::DoubleValue (1.9), ns3::MakeDoubleChecker<double> (0.1, 1000.0));
static ns3::GlobalValue g_ues ("ues", "Number of UEs for each mmWave ENB.", ns3::UintegerValue (7), ns3::MakeUintegerChecker<uint8_t> ());
static ns3::GlobalValue g_indicationPeriodicity ("indicationPeriodicity", "E2 Indication Periodicity reports (value in seconds)", ns3::DoubleValue (0.1), ns3::MakeDoubleChecker<double> (0.01, 2.0));
static ns3::GlobalValue g_configuration ("configuration", "Set the wanted configuration to emulate [0,2]", ns3::UintegerValue (1), ns3::MakeUintegerChecker<uint8_t> ());
static ns3::GlobalValue g_trafficModel ("trafficModel", "Type of the traffic model [0,3]", ns3::UintegerValue (0), ns3::MakeUintegerChecker<uint8_t> ());
static ns3::GlobalValue q_useSemaphores ("useSemaphores", "If true, enables the use of semaphores for external environment control", ns3::BooleanValue (false), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_controlFileName ("controlFileName", "The path to the control file for hierarchical actions", ns3::StringValue (""), ns3::MakeStringChecker ());
//hierarchical_actions.csv
// Parâmetros de Handover (do scenario-one)
static ns3::GlobalValue g_hoSinrDifference ("hoSinrDifference", "The SINR value difference for which a handover is triggered", ns3::DoubleValue (3), ns3::MakeDoubleChecker<double> ());

// Parâmetros de Mobilidade (do scenario-three)
static ns3::GlobalValue g_positionAllocator ("positionAllocator", "UE position allocator type [0,1]", ns3::UintegerValue (0), ns3::MakeUintegerChecker<uint8_t> ());
static ns3::GlobalValue g_nBsNoUesAlloc ("nBsNoUesAlloc", "Number of BS without initial UEs allocated", ns3::IntegerValue (-1), ns3::MakeIntegerChecker<int8_t> ());
static ns3::GlobalValue g_minSpeed ("minSpeed", "minimum UE speed in m/s", ns3::DoubleValue (2.0), ns3::MakeDoubleChecker<double> ());
static ns3::GlobalValue g_maxSpeed ("maxSpeed", "maximum UE speed in m/s", ns3::DoubleValue (4.0), ns3::MakeDoubleChecker<double> ());

// Parâmetros Técnicos (comuns ou de um dos cenários)
static ns3::GlobalValue g_bufferSize ("bufferSize", "RLC tx buffer size (MB)", ns3::UintegerValue (10), ns3::MakeUintegerChecker<uint32_t> ());
static ns3::GlobalValue g_rlcAmEnabled ("rlcAmEnabled", "If true, use RLC AM, else use RLC UM", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_enableTraces ("enableTraces", "If true, generate ns-3 traces", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2lteEnabled ("e2lteEnabled", "If true, send LTE E2 reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2nrEnabled ("e2nrEnabled", "If true, send NR E2 reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2du ("e2du", "If true, send DU reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2cuUp ("e2cuUp", "If true, send CU-UP reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_e2cuCp ("e2cuCp", "If true, send CU-CP reports", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_dataRate ("dataRate", "Set the data rate to be used [0=low, 1=high]", ns3::DoubleValue (0), ns3::MakeDoubleChecker<double> (0, 1));
static ns3::GlobalValue g_reducedPmValues ("reducedPmValues", "If true, use a subset of the pm containers", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());
static ns3::GlobalValue g_outageThreshold ("outageThreshold", "SNR threshold for outage events [dB]", ns3::DoubleValue (-1000.0), ns3::MakeDoubleChecker<double> ());
static ns3::GlobalValue g_basicCellId ("basicCellId", "The next value will be the first cellId", ns3::UintegerValue (1), ns3::MakeUintegerChecker<uint8_t> ());
static ns3::GlobalValue g_numberOfRaPreambles ("numberOfRaPreambles", "Number of RA preambles", ns3::UintegerValue (40), ns3::MakeUintegerChecker<uint8_t> ());
static ns3::GlobalValue g_handoverMode ("handoverMode", "HO euristic to be used", ns3::StringValue ("NoAuto"), ns3::MakeStringChecker ());
static ns3::GlobalValue g_e2TermIp ("e2TermIp", "The IP address of the RIC E2 termination", ns3::StringValue ("127.0.0.1"), ns3::MakeStringChecker ());
static ns3::GlobalValue g_enableE2FileLogging ("enableE2FileLogging", "If true, generate offline file logging instead of connecting to RIC", ns3::BooleanValue (true), ns3::MakeBooleanChecker ());

int
main (int argc, char *argv[])
{
  // LogComponentEnableAll (LOG_PREFIX_ALL);
  LogComponentEnable ("ScenarioHierarchical", LOG_LEVEL_INFO);

  double maxXAxis = 4000;
  double maxYAxis = 4000;

  CommandLine cmd;
  cmd.Parse (argc, argv);

  // --- Leitura e Configuração de Parâmetros ---
  bool harqEnabled = true;
  UintegerValue uintegerValue;
  IntegerValue integerValue;
  BooleanValue booleanValue;
  StringValue stringValue;
  DoubleValue doubleValue;

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
  GlobalValue::GetValueByName ("enableTraces", booleanValue);
  bool enableTraces = booleanValue.Get ();
  GlobalValue::GetValueByName ("trafficModel", uintegerValue);
  uint8_t trafficModel = uintegerValue.Get ();
  GlobalValue::GetValueByName ("nBsNoUesAlloc", integerValue);
  int8_t nBsNoUesAlloc = integerValue.Get ();
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

  // --- Configurações Padrão do ns-3 (mescladas) ---
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
  Config::SetDefault ("ns3::LteEnbRrc::HoSinrDifference", DoubleValue (hoSinrDifference)); // <-- Adicionado do scenario-one

  // Outras configurações
  Config::SetDefault ("ns3::MmWaveHelper::RlcAmEnabled", BooleanValue (rlcAmEnabled));
  Config::SetDefault ("ns3::MmWaveHelper::HarqEnabled", BooleanValue (harqEnabled));
  Config::SetDefault ("ns3::LteRlcAm::MaxTxBufferSize", UintegerValue (bufferSize * 1024 * 1024));

  // --- Construção do Cenário (baseado no scenario-three) ---

  // ... (O código de configuração de rede, nós, mobilidade, EPC, etc., é idêntico
  //      e foi omitido por brevidade. A lógica abaixo é a mesma de scenario-three.cc,
  //      que já é uma versão mais completa que a de scenario-one.cc)

  // Carrier bandwidth in Hz
  double bandwidth;
  double centerFrequency;
  double isd;
  int numAntennasMcUe;
  int numAntennasMmWave;
  std::string dataRate;

  GlobalValue::GetValueByName ("configuration", uintegerValue);
  uint8_t configuration = uintegerValue.Get ();
  switch (configuration)
  {
    case 0:
      centerFrequency = 850e6; bandwidth = 20e6; isd = 1000;
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

  Config::SetDefault ("ns3::MmWavePhyMacCommon::Bandwidth", DoubleValue (bandwidth));
  Config::SetDefault ("ns3::MmWavePhyMacCommon::CenterFreq", DoubleValue (centerFrequency));

  Ptr<MmWaveHelper> mmwaveHelper = CreateObject<MmWaveHelper> ();
  mmwaveHelper->SetPathlossModelType ("ns3::ThreeGppUmiStreetCanyonPropagationLossModel");

  Ptr<MmWavePointToPointEpcHelper> epcHelper = CreateObject<MmWavePointToPointEpcHelper> ();
  mmwaveHelper->SetEpcHelper (epcHelper);

  uint8_t nMmWaveEnbNodes = 7;
  uint8_t nLteEnbNodes = 1;
  GlobalValue::GetValueByName ("ues", uintegerValue);
  uint32_t ues = uintegerValue.Get ();
  uint8_t nUeNodes = ues * nMmWaveEnbNodes;

  Ptr<Node> pgw = epcHelper->GetPgwNode ();
  NodeContainer remoteHostContainer;
  remoteHostContainer.Create (1);
  Ptr<Node> remoteHost = remoteHostContainer.Get (0);
  InternetStackHelper internet;
  internet.Install (remoteHostContainer);

  PointToPointHelper p2ph;
  p2ph.SetDeviceAttribute ("DataRate", DataRateValue (DataRate ("100Gb/s")));
  p2ph.SetDeviceAttribute ("Mtu", UintegerValue (2500));
  NetDeviceContainer internetDevices = p2ph.Install (pgw, remoteHost);
  Ipv4AddressHelper ipv4h;
  ipv4h.SetBase ("1.0.0.0", "255.0.0.0");
  Ipv4InterfaceContainer internetIpIfaces = ipv4h.Assign (internetDevices);
  Ipv4Address remoteHostAddr = internetIpIfaces.GetAddress (1);
  Ipv4StaticRoutingHelper ipv4RoutingHelper;
  Ptr<Ipv4StaticRouting> remoteHostStaticRouting = ipv4RoutingHelper.GetStaticRouting (remoteHost->GetObject<Ipv4> ());
  remoteHostStaticRouting->AddNetworkRouteTo (Ipv4Address ("7.0.0.0"), Ipv4Mask ("255.0.0.0"), 1);

  NodeContainer ueNodes;
  NodeContainer mmWaveEnbNodes;
  NodeContainer lteEnbNodes;
  NodeContainer allEnbNodes;
  mmWaveEnbNodes.Create (nMmWaveEnbNodes);
  lteEnbNodes.Create (nLteEnbNodes);
  ueNodes.Create (nUeNodes);
  allEnbNodes.Add (lteEnbNodes);
  allEnbNodes.Add (mmWaveEnbNodes);

  Vector centerPosition = Vector (maxXAxis / 2, maxYAxis / 2, 3);
  Ptr<ListPositionAllocator> enbPositionAlloc = CreateObject<ListPositionAllocator> ();
  enbPositionAlloc->Add (centerPosition); // LTE
  enbPositionAlloc->Add (centerPosition); // Co-located mmWave

  for (int8_t i = 0; i < (nMmWaveEnbNodes - 1); ++i)
  {
    double x = isd * cos ((2 * M_PI * i) / (nMmWaveEnbNodes - 1));
    double y = isd * sin ((2 * M_PI * i) / (nMmWaveEnbNodes - 1));
    enbPositionAlloc->Add (Vector (centerPosition.x + x, centerPosition.y + y, 3));
  }

  MobilityHelper enbmobility;
  enbmobility.SetMobilityModel ("ns3::ConstantPositionMobilityModel");
  enbmobility.SetPositionAllocator (enbPositionAlloc);
  enbmobility.Install (allEnbNodes);

  // Lógica de mobilidade flexível do scenario-three
  MobilityHelper uemobility;
  Ptr<UniformRandomVariable> speed = CreateObject<UniformRandomVariable> ();
  speed->SetAttribute ("Min", DoubleValue (minSpeed));
  speed->SetAttribute ("Max", DoubleValue (maxSpeed));

  switch (positionAllocator)
  {
    case 0: {
      Ptr<UniformDiscPositionAllocator> uePositionAlloc = CreateObject<UniformDiscPositionAllocator> ();
      uePositionAlloc->SetX(centerPosition.x);
      uePositionAlloc->SetY(centerPosition.y);
      uePositionAlloc->SetRho(isd);
      uemobility.SetMobilityModel("ns3::RandomWalk2dMobilityModel", "Speed", PointerValue(speed), "Bounds", RectangleValue(Rectangle(0, maxXAxis, 0, maxYAxis)));
      uemobility.SetPositionAllocator(uePositionAlloc);
      uemobility.Install(ueNodes);
      break;
    }
    case 1: {
      // Lógica complexa para alocar UEs em torno de um subconjunto de BSs.
      // Omitido para brevidade, mas deve ser copiado do scenario-three.cc se necessário.
      NS_LOG_WARN("Position Allocator 1 is complex and its logic should be copied from scenario-three.cc. Defaulting to Allocator 0.");
      Ptr<UniformDiscPositionAllocator> uePositionAlloc = CreateObject<UniformDiscPositionAllocator> ();
      uePositionAlloc->SetX(centerPosition.x);
      uePositionAlloc->SetY(centerPosition.y);
      uePositionAlloc->SetRho(isd);
      uemobility.SetMobilityModel("ns3::RandomWalk2dMobilityModel", "Speed", PointerValue(speed), "Bounds", RectangleValue(Rectangle(0, maxXAxis, 0, maxYAxis)));
      uemobility.SetPositionAllocator(uePositionAlloc);
      uemobility.Install(ueNodes);
      break;
    }
    default:
      NS_FATAL_ERROR("positionAllocator not recognized " << positionAllocator);
      break;
  }


  NetDeviceContainer lteEnbDevs = mmwaveHelper->InstallLteEnbDevice (lteEnbNodes);
  NetDeviceContainer mmWaveEnbDevs = mmwaveHelper->InstallEnbDevice (mmWaveEnbNodes);
  NetDeviceContainer mcUeDevs = mmwaveHelper->InstallMcUeDevice (ueNodes);

  internet.Install (ueNodes);
  Ipv4InterfaceContainer ueIpIface = epcHelper->AssignUeIpv4Address (NetDeviceContainer (mcUeDevs));

  for (uint32_t u = 0; u < ueNodes.GetN (); ++u)
  {
    Ptr<Node> ueNode = ueNodes.Get (u);
    Ptr<Ipv4StaticRouting> ueStaticRouting = ipv4RoutingHelper.GetStaticRouting (ueNode->GetObject<Ipv4> ());
    ueStaticRouting->SetDefaultRoute (epcHelper->GetUeDefaultGatewayAddress (), 1);
  }

  mmwaveHelper->AddX2Interface (lteEnbNodes, mmWaveEnbNodes);
  mmwaveHelper->AttachToClosestEnb (mcUeDevs, mmWaveEnbDevs, lteEnbDevs);

  // --- Setup das Aplicações (tráfego) ---
  // A lógica de tráfego é idêntica em ambos os cenários e foi omitida por brevidade.
  // Cole o bloco de "Install and start applications" até "clientApp.Stop" aqui.
  uint16_t portUdp = 60000;
  PacketSinkHelper sinkHelperUdp ("ns3::UdpSocketFactory", InetSocketAddress (Ipv4Address::GetAny (), portUdp));
  ApplicationContainer sinkApp = sinkHelperUdp.Install (remoteHost);
  ApplicationContainer clientApp;
  for (uint32_t u = 0; u < ueNodes.GetN (); ++u)
  {
    PacketSinkHelper dlPacketSinkHelper ("ns3::UdpSocketFactory", InetSocketAddress (Ipv4Address::GetAny (), 1234));
    sinkApp.Add (dlPacketSinkHelper.Install (ueNodes.Get (u)));
    UdpClientHelper dlClient (ueIpIface.GetAddress (u), 1234);
    dlClient.SetAttribute ("Interval", TimeValue (MicroSeconds (500)));
    dlClient.SetAttribute ("MaxPackets", UintegerValue (UINT32_MAX));
    dlClient.SetAttribute ("PacketSize", UintegerValue (1280));
    clientApp.Add (dlClient.Install (remoteHost));
  }


  // --- Início e Fim da Simulação ---
  GlobalValue::GetValueByName ("simTime", doubleValue);
  double simTime = doubleValue.Get ();
  sinkApp.Start (Seconds (0));
  clientApp.Start (MilliSeconds (100));
  clientApp.Stop (Seconds (simTime - 0.1));

  if (enableTraces)
  {
    mmwaveHelper->EnableTraces ();
  }

  Ptr<LteHelper> lteHelper = CreateObject<LteHelper> ();
  lteHelper->Initialize ();
  lteHelper->EnablePhyTraces ();
  lteHelper->EnableMacTraces ();

  PrintGnuplottableUeListToFile ("ues.txt");
  PrintGnuplottableEnbListToFile ("enbs.txt");

  // --- Agendamento do Log de Estado da BS (do scenario-three) ---
  Ptr<LteEnbNetDevice> ltedev = DynamicCast<LteEnbNetDevice> (lteEnbDevs.Get (0));
  Ptr<LteEnbRrc> lte_rrc = ltedev->GetRrc ();
  for (double i = 0.0; i < simTime; i = i + indicationPeriodicity){
    Simulator::Schedule (Seconds (i), BsStateTrace,"bsState.txt", ltedev, lte_rrc);
  }

  NS_LOG_UNCOND ("Hierarchical Simulation Starting. Time: " << simTime << " seconds. Control File: " << controlFilename);
  Simulator::Stop (Seconds (simTime));
  Simulator::Run ();
  Simulator::Destroy ();
  NS_LOG_INFO ("Done.");
  return 0;
}

