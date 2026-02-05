/* -*-  Mode: C++; c-file-style: "gnu"; indent-tabs-mode:nil; -*- */
/*
 * Este programa é software livre; você pode redistribuí-lo e/ou modificá-lo
 * sob os termos da Licença Pública Geral GNU versão 2.
 *
 * Autores Originais: Andrea Lacava <thecave003@gmail.com>
 * Michele Polese <michele.polese@gmail.com>
 *
 * Adaptação para Pesquisa de Mestrado (Jussilea Gurjão de Figueiredo):
 * Desenvolvedor: Elioth Luy Almeida da Silva (Iniciação Científica)
 * * Descrição das Modificações para Gerenciamento de QoS Dinâmico:
 * - Intensificação de tráfego para treinamento de modelos DRL.
 * - Adição de taxas de dados configuráveis por perfil de tráfego (Slices).
 * - Implementação de logging para métricas de proxy de energia.
 * - Otimização para experimentos de trade-off entre Latência vs. Energia.
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

using namespace ns3;
using namespace mmwave;

/**
 * Scenario QoS - Dynamic Slicing for DRL Training
 *
 * This scenario is designed to create network congestion and resource
 * scarcity conditions that force the DRL agent to learn meaningful
 * policies for balancing latency and energy consumption.
 *
 * Key modifications from scenario-two:
 * 1. Higher eMBB data rates to saturate the network
 * 2. More frequent URLLC bursts to create latency-sensitive traffic
 * 3. Configurable traffic parameters via command line
 * 4. Enhanced logging for energy proxy metrics
 */

NS_LOG_COMPONENT_DEFINE ("ScenarioQoS");

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
          Ptr<LteUeNetDevice> uedev = node->GetDevice (j)->GetObject<LteUeNetDevice> ();
          Ptr<MmWaveUeNetDevice> mmuedev = node->GetDevice (j)->GetObject<MmWaveUeNetDevice> ();
          Ptr<McUeNetDevice> mcuedev = node->GetDevice (j)->GetObject<McUeNetDevice> ();
          if (uedev)
            {
              Vector pos = node->GetObject<MobilityModel> ()->GetPosition ();
              outFile << "set label \"" << uedev->GetImsi () << "\" at " << pos.x << "," << pos.y
                      << " left font \"sans,8\" textcolor rgb \"black\" front point pt 1 ps "
                         "0.3 lc rgb \"black\" offset 0,0"
                      << std::endl;
            }
          else if (mmuedev)
            {
              Vector pos = node->GetObject<MobilityModel> ()->GetPosition ();
              outFile << "set label \"" << mmuedev->GetImsi () << "\" at " << pos.x << "," << pos.y
                      << " left font \"sans,8\" textcolor rgb \"black\" front point pt 1 ps "
                         "0.3 lc rgb \"black\" offset 0,0"
                      << std::endl;
            }
          else if (mcuedev)
            {
              Vector pos = node->GetObject<MobilityModel> ()->GetPosition ();
              outFile << "set label \"" << mcuedev->GetImsi () << "\" at " << pos.x << "," << pos.y
                      << " left font \"sans,8\" textcolor rgb \"black\" front point pt 1 ps "
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
                      << " left font \"sans,8\" textcolor rgb \"blue\" front  point pt 4 ps "
                         "0.3 lc rgb \"blue\" offset 0,0"
                      << std::endl;
            }
          else if (mmdev)
            {
              Vector pos = node->GetObject<MobilityModel> ()->GetPosition ();
              outFile << "set label \"" << mmdev->GetCellId () << "\" at " << pos.x << "," << pos.y
                      << " left font \"sans,8\" textcolor rgb \"red\" front  point pt 4 ps "
                         "0.3 lc rgb \"red\" offset 0,0"
                      << std::endl;
            }
        }
    }
}

/**
 * Save QoS Load Balance metrics to file
 * This is essential for the Python environment to calculate lte_load_balance
 */
void
SaveLoadBalanceQoSValueToFile (std::string filename, Ptr<LteEnbNetDevice> lteEnb,
                                std::vector<Ptr<MmWaveEnbNetDevice>> mmWaveGnbs)
{
  std::ofstream outFile;

  NS_LOG_LOGIC ("Trigger Save Load balancing at " << Simulator::Now ().GetMilliSeconds ());

  outFile.open (filename.c_str (), std::ios_base::app);
  if (!outFile.is_open ())
    {
      NS_LOG_ERROR ("Can't open file " << filename);
      return;
    }

  std::map<uint16_t, double>
      loadBalancingMap; // key is the mmWave cell Id, value is the QoS Load balancing for that cell

  // Calculate the Load balancing of the LTE Cell
  double lteLoadBalance = 0.0;
  double ueQos;
  for (auto ue : lteEnb->GetRrc ()->GetUeMap ())
    {
      ueQos = lteEnb->GetUeQoS (ue.first);
      lteLoadBalance += ueQos;
      NS_LOG_LOGIC ("LTE Load Balance for " << ue.second->GetImsi () << " is " << ueQos
                                           << ", complex (cell " << lteEnb->GetCellId ()
                                           << ") : " << lteLoadBalance);
    }

  // Calculate mmWave load balance (1 - LTE QoS)
  double mmwaLoadBalance;
  for (auto mmWaveGnb : mmWaveGnbs)
    {
      mmwaLoadBalance = 0.0;
      for (auto ue : mmWaveGnb->GetRrc ()->GetUeMap ())
        {
          ueQos = lteEnb->GetUeQoS (ue.second->GetImsi ());
          mmwaLoadBalance += (1 - ueQos);
          NS_LOG_LOGIC ("MmWave Load Balance for " << ue.second->GetImsi () << " is " << 1 - ueQos
                                                  << ", complex: " << mmwaLoadBalance);
        }
      loadBalancingMap[mmWaveGnb->GetCellId ()] = mmwaLoadBalance;
    }

  // Log to file
  // Format: timestamp,ueImsiComplete,QoS.CellLoadBalance.Lte,NrCellId,QoS.CellLoadBalance.Nr
  uint64_t timestamp = lteEnb->GetStartTime () + (uint64_t) Simulator::Now ().GetMilliSeconds ();
  std::string ueImsiString;
  for (auto mmWaveGnb : mmWaveGnbs)
    {
      for (auto ue : mmWaveGnb->GetRrc ()->GetUeMap ())
        {
          ueImsiString = mmWaveGnb->GetImsiString (ue.second->GetImsi());
          outFile << std::to_string (timestamp) << "," << ueImsiString << ","
                  << std::to_string (lteLoadBalance) << ","
                  << std::to_string (mmWaveGnb->GetCellId ()) << ","
                  << std::to_string (loadBalancingMap[mmWaveGnb->GetCellId ()]) << std::endl;
        }
    }

  outFile.close ();
}

// ==================== GLOBAL VALUES ====================
// Buffer size - keep at 10MB to allow bufferbloat and measurable latency
static ns3::GlobalValue g_bufferSize ("bufferSize", "RLC tx buffer size (MB)",
                                      ns3::UintegerValue (10),
                                      ns3::MakeUintegerChecker<uint32_t> ());

static ns3::GlobalValue g_rlcAmEnabled ("rlcAmEnabled", "If true, use RLC AM, else use RLC UM",
                                        ns3::BooleanValue (true), ns3::MakeBooleanChecker ());

// Traffic type percentages
static ns3::GlobalValue g_PercUEeMBB ("PercUEeMBB",
                                        "Percentage of UEs to deploy for eMBB traffic model",
                                        ns3::DoubleValue (0.4),  // Increased from 0.3
                                        ns3::MakeDoubleChecker<double> ());

static ns3::GlobalValue g_PercUEURLLC ("PercUEURLLC",
                                        "Percentage of UEs to deploy for URLLC traffic model",
                                        ns3::DoubleValue (0.3),
                                        ns3::MakeDoubleChecker<double> ());

// QoS split percentages (initial values, controlled by agent)
static ns3::GlobalValue g_qoSeMBB ("qoSeMBB",
                                        "Initial QoS split for eMBB (-1 means agent controlled)",
                                        ns3::DoubleValue (-1),
                                        ns3::MakeDoubleChecker<double> (-1, 1.0));

static ns3::GlobalValue g_qoSURLLC ("qoSURLLC",
                                        "Initial QoS split for URLLC (-1 means agent controlled)",
                                        ns3::DoubleValue (-1),
                                        ns3::MakeDoubleChecker<double> (-1, 1.0));

static ns3::GlobalValue g_qoSmIoT ("qoSmIoT",
                                        "Initial QoS split for mIoT (-1 means agent controlled)",
                                        ns3::DoubleValue (-1),
                                        ns3::MakeDoubleChecker<double> (-1, 1.0));

// ==================== NEW: TRAFFIC INTENSITY PARAMETERS ====================
// These allow fine-tuning the network load via command line

static ns3::GlobalValue g_embbDataRate ("embbDataRate",
                                        "Data rate for eMBB traffic in Mbps (use high values to saturate)",
                                        ns3::DoubleValue (50.0),  // 50 Mbps per eMBB user
                                        ns3::MakeDoubleChecker<double> (0.1, 1000.0));

static ns3::GlobalValue g_urllcDataRate ("urllcDataRate",
                                        "Data rate for URLLC traffic in kbps",
                                        ns3::DoubleValue (500.0),  // Increased from 89.3 kbps
                                        ns3::MakeDoubleChecker<double> (1.0, 10000.0));

static ns3::GlobalValue g_miotDataRate ("miotDataRate",
                                        "Data rate for mIoT traffic in kbps",
                                        ns3::DoubleValue (100.0),  // Increased from 44.6 kbps
                                        ns3::MakeDoubleChecker<double> (1.0, 1000.0));

static ns3::GlobalValue g_urllcBurstMean ("urllcBurstMean",
                                        "Mean ON time for URLLC bursts in seconds",
                                        ns3::DoubleValue (0.005),  // 5ms bursts (more frequent)
                                        ns3::MakeDoubleChecker<double> (0.001, 1.0));

static ns3::GlobalValue g_urllcOffMean ("urllcOffMean",
                                        "Mean OFF time for URLLC in seconds",
                                        ns3::DoubleValue (0.01),  // 10ms between bursts
                                        ns3::MakeDoubleChecker<double> (0.0, 1.0));

// ==================== CONTROL PARAMETERS ====================
static ns3::GlobalValue q_useSemaphores ("useSemaphores",
                                        "If true, enables semaphores for external environment control (REQUIRED for Gym)",
                                        ns3::BooleanValue (true), ns3::MakeBooleanChecker ());

static ns3::GlobalValue g_configuration ("configuration", "Set the RF configuration [0,2]",
                                         ns3::UintegerValue (1),
                                         ns3::MakeUintegerChecker<uint8_t> ());

static ns3::GlobalValue g_ues ("ues", "Number of UEs for each mmWave gNB.",
                               ns3::UintegerValue (10),  // Increased from 7 for higher density
                               ns3::MakeUintegerChecker<uint8_t> ());

static ns3::GlobalValue g_simTime ("simTime", "Simulation time in seconds",
                                   ns3::DoubleValue (10.0),  // Longer episodes for DRL
                                   ns3::MakeDoubleChecker<double> (0.1, 1000.0));

static ns3::GlobalValue g_policy ("policy",
                                  "Policy type: 0=SingleAgent, 1=Random, 2=Throughput, 3=SINR, 4=MultiAgent",
                                  ns3::UintegerValue (0),
                                  ns3::MakeUintegerChecker<uint8_t> ());

static ns3::GlobalValue g_controlFileName ("controlFileName",
                                     "Path to control file for agent actions",
                                     ns3::StringValue ("qos_actions.csv"),
                                     ns3::MakeStringChecker ());

static ns3::GlobalValue g_indicationPeriodicity ("indicationPeriodicity",
                                   "E2 Indication Periodicity in seconds",
                                   ns3::DoubleValue (0.1),
                                   ns3::MakeDoubleChecker<double> (0.01, 2.0));

// ==================== MAIN FUNCTION ====================
int
main (int argc, char *argv[])
{
  LogComponentEnableAll (LOG_PREFIX_ALL);
  LogComponentEnable ("ScenarioQoS", LOG_LEVEL_INFO);
  LogComponentEnable ("LteEnbNetDevice", LOG_LEVEL_INFO);
  LogComponentEnable ("MmWaveEnbNetDevice", LOG_LEVEL_INFO);
  LogComponentEnable ("MmWaveBearerStatsCalculator", LOG_LEVEL_FUNCTION);
  LogComponentEnable ("LteStatsCalculator", LOG_LEVEL_FUNCTION);
  LogComponentEnable ("RadioBearerStatsCalculator", LOG_LEVEL_FUNCTION);
  LogComponentEnable ("LteRlcAm", LOG_LEVEL_FUNCTION);
  LogComponentEnable ("MmWaveBearerStatsConnector", LOG_LEVEL_FUNCTION);
  LogComponentEnable ("RadioBearerStatsConnector", LOG_LEVEL_FUNCTION);
  LogComponentEnable ("LteEnbRrc", LOG_LEVEL_INFO);
  LogComponentEnable ("McEnbPdcp", LOG_LEVEL_FUNCTION);
  LogComponentEnable ("McUePdcp", LOG_LEVEL_INFO);

  // Scenario dimensions
  double maxXAxis = 4000;
  double maxYAxis = 4000;

  // Parse command line arguments
  CommandLine cmd;
  cmd.Parse (argc, argv);

  bool harqEnabled = true;

  // Get global values
  UintegerValue uintegerValue;
  BooleanValue booleanValue;
  StringValue stringValue;
  DoubleValue doubleValue;

  GlobalValue::GetValueByName ("rlcAmEnabled", booleanValue);
  bool rlcAmEnabled = booleanValue.Get ();
  GlobalValue::GetValueByName ("useSemaphores", booleanValue);
  bool useSemaphores = booleanValue.Get ();
  GlobalValue::GetValueByName ("bufferSize", uintegerValue);
  uint32_t bufferSize = uintegerValue.Get ();
  GlobalValue::GetValueByName ("PercUEeMBB", doubleValue);
  double PercUEeMBB = doubleValue.Get ();
  GlobalValue::GetValueByName ("PercUEURLLC", doubleValue);
  double PercUEURLLC = doubleValue.Get ();
  GlobalValue::GetValueByName ("qoSeMBB", doubleValue);
  double qoSeMBB = doubleValue.Get ();
  GlobalValue::GetValueByName ("qoSURLLC", doubleValue);
  double qoSURLLC = doubleValue.Get ();
  GlobalValue::GetValueByName ("qoSmIoT", doubleValue);
  double qoSmIoT = doubleValue.Get ();
  GlobalValue::GetValueByName ("controlFileName", stringValue);
  std::string controlFilename = stringValue.Get ();
  GlobalValue::GetValueByName ("indicationPeriodicity", doubleValue);
  double indicationPeriodicity = doubleValue.Get ();

  // Get traffic intensity parameters
  GlobalValue::GetValueByName ("embbDataRate", doubleValue);
  double embbDataRateMbps = doubleValue.Get ();
  GlobalValue::GetValueByName ("urllcDataRate", doubleValue);
  double urllcDataRateKbps = doubleValue.Get ();
  GlobalValue::GetValueByName ("miotDataRate", doubleValue);
  double miotDataRateKbps = doubleValue.Get ();
  GlobalValue::GetValueByName ("urllcBurstMean", doubleValue);
  double urllcBurstMean = doubleValue.Get ();
  GlobalValue::GetValueByName ("urllcOffMean", doubleValue);
  double urllcOffMean = doubleValue.Get ();

  // Validate percentages
  if (PercUEeMBB + PercUEURLLC > 1)
    {
      NS_FATAL_ERROR ("The total percentage of UEs for each traffic model is higher than 1: "
                      << PercUEeMBB + PercUEURLLC);
    }

  NS_LOG_INFO ("========== SCENARIO QoS CONFIGURATION ==========");
  NS_LOG_INFO ("rlcAmEnabled: " << rlcAmEnabled);
  NS_LOG_INFO ("bufferSize: " << bufferSize << " MB");
  NS_LOG_INFO ("useSemaphores: " << useSemaphores);
  NS_LOG_INFO ("controlFilename: " << controlFilename);
  NS_LOG_INFO ("indicationPeriodicity: " << indicationPeriodicity << " s");
  NS_LOG_INFO ("--- Traffic Distribution ---");
  NS_LOG_INFO ("PercUEeMBB: " << PercUEeMBB * 100 << "%");
  NS_LOG_INFO ("PercUEURLLC: " << PercUEURLLC * 100 << "%");
  NS_LOG_INFO ("PercUEmIoT: " << (1 - PercUEeMBB - PercUEURLLC) * 100 << "%");
  NS_LOG_INFO ("--- Traffic Intensity ---");
  NS_LOG_INFO ("eMBB DataRate: " << embbDataRateMbps << " Mbps per UE");
  NS_LOG_INFO ("URLLC DataRate: " << urllcDataRateKbps << " kbps per UE");
  NS_LOG_INFO ("mIoT DataRate: " << miotDataRateKbps << " kbps per UE");
  NS_LOG_INFO ("URLLC Burst Mean: " << urllcBurstMean * 1000 << " ms");
  NS_LOG_INFO ("URLLC Off Mean: " << urllcOffMean * 1000 << " ms");
  NS_LOG_INFO ("--- QoS Initial Split ---");
  NS_LOG_INFO ("qoSeMBB: " << (qoSeMBB == -1 ? "Agent Controlled" : std::to_string(qoSeMBB)));
  NS_LOG_INFO ("qoSURLLC: " << (qoSURLLC == -1 ? "Agent Controlled" : std::to_string(qoSURLLC)));
  NS_LOG_INFO ("qoSmIoT: " << (qoSmIoT == -1 ? "Agent Controlled" : std::to_string(qoSmIoT)));
  NS_LOG_INFO ("================================================");

  // Configure ns-3 defaults
  Config::SetDefault ("ns3::MmWaveHelper::E2ModeLte", BooleanValue(true));
  Config::SetDefault ("ns3::MmWaveHelper::E2ModeNr", BooleanValue(true));
  Config::SetDefault ("ns3::MmWaveHelper::E2Periodicity", DoubleValue (indicationPeriodicity));

  Config::SetDefault ("ns3::MmWaveEnbNetDevice::EnableDuReport", BooleanValue(true));
  Config::SetDefault ("ns3::LteEnbNetDevice::ControlFileName", StringValue (controlFilename));
  Config::SetDefault ("ns3::LteEnbNetDevice::UseSemaphores", BooleanValue (useSemaphores));
  Config::SetDefault ("ns3::MmWaveEnbNetDevice::EnableCuUpReport", BooleanValue(true));
  Config::SetDefault ("ns3::LteEnbNetDevice::EnableCuUpReport", BooleanValue(true));
  Config::SetDefault ("ns3::MmWaveEnbNetDevice::EnableCuCpReport", BooleanValue(true));
  Config::SetDefault ("ns3::LteEnbNetDevice::EnableCuCpReport", BooleanValue(true));
  Config::SetDefault ("ns3::LteEnbNetDevice::EnableE2FileLogging", BooleanValue (true));
  Config::SetDefault ("ns3::MmWaveEnbNetDevice::EnableE2FileLogging", BooleanValue (true));

  Config::SetDefault ("ns3::MmWaveEnbMac::NumberOfRaPreambles", UintegerValue (40));
  Config::SetDefault ("ns3::LteEnbMac::NumberOfRaPreambles", UintegerValue (40));

  Config::SetDefault ("ns3::MmWaveHelper::RlcAmEnabled", BooleanValue (rlcAmEnabled));
  Config::SetDefault ("ns3::MmWaveHelper::HarqEnabled", BooleanValue (harqEnabled));
  Config::SetDefault ("ns3::MmWaveHelper::UseIdealRrc", BooleanValue (true));

  Config::SetDefault ("ns3::MmWaveFlexTtiMacScheduler::HarqEnabled", BooleanValue (harqEnabled));
  Config::SetDefault ("ns3::MmWavePhyMacCommon::NumHarqProcess", UintegerValue (100));

  Config::SetDefault ("ns3::ThreeGppChannelModel::UpdatePeriod", TimeValue (MilliSeconds (100.0)));
  Config::SetDefault ("ns3::ThreeGppChannelConditionModel::UpdatePeriod", TimeValue (MilliSeconds (100)));

  Config::SetDefault ("ns3::LteRlcAm::ReportBufferStatusTimer", TimeValue (MilliSeconds (10.0)));
  Config::SetDefault ("ns3::LteRlcUmLowLat::ReportBufferStatusTimer", TimeValue (MilliSeconds (10.0)));
  Config::SetDefault ("ns3::LteRlcUm::MaxTxBufferSize", UintegerValue (bufferSize * 1024 * 1024));
  Config::SetDefault ("ns3::LteRlcUmLowLat::MaxTxBufferSize", UintegerValue (bufferSize * 1024 * 1024));
  Config::SetDefault ("ns3::LteRlcAm::MaxTxBufferSize", UintegerValue (bufferSize * 1024 * 1024));

  // RF Configuration
  double bandwidth;
  double centerFrequency;
  double isd;
  int numAntennasMcUe;
  int numAntennasMmWave;

  GlobalValue::GetValueByName ("configuration", uintegerValue);
  uint8_t configuration = uintegerValue.Get ();
  switch (configuration)
    {
    case 0:  // Sub-6 GHz (low capacity)
      centerFrequency = 850e6;
      bandwidth = 20e6;
      isd = 1000;
      numAntennasMcUe = 1;
      numAntennasMmWave = 1;
      break;

    case 1:  // Mid-band (balanced)
      centerFrequency = 3.5e9;
      bandwidth = 100e6;  // Increased bandwidth
      isd = 500;          // Reduced ISD for denser deployment
      numAntennasMcUe = 4;
      numAntennasMmWave = 16;
      break;

    case 2:  // mmWave (high capacity)
      centerFrequency = 28e9;
      bandwidth = 400e6;  // Maximum mmWave bandwidth
      isd = 200;
      numAntennasMcUe = 16;
      numAntennasMmWave = 64;
      break;

    default:
      NS_FATAL_ERROR ("Configuration not recognized: " << configuration);
      break;
    }

  Config::SetDefault ("ns3::MmWavePhyMacCommon::Bandwidth", DoubleValue (bandwidth));
  Config::SetDefault ("ns3::MmWavePhyMacCommon::CenterFreq", DoubleValue (centerFrequency));

  Ptr<MmWaveHelper> mmwaveHelper = CreateObject<MmWaveHelper> ();
  mmwaveHelper->SetPathlossModelType ("ns3::ThreeGppUmiStreetCanyonPropagationLossModel");
  mmwaveHelper->SetChannelConditionModelType ("ns3::ThreeGppUmiStreetCanyonChannelConditionModel");

  mmwaveHelper->SetUePhasedArrayModelAttribute("NumColumns", UintegerValue(std::sqrt(numAntennasMcUe)));
  mmwaveHelper->SetUePhasedArrayModelAttribute("NumRows", UintegerValue(std::sqrt(numAntennasMcUe)));
  mmwaveHelper->SetEnbPhasedArrayModelAttribute("NumColumns", UintegerValue(std::sqrt(numAntennasMmWave)));
  mmwaveHelper->SetEnbPhasedArrayModelAttribute("NumRows", UintegerValue(std::sqrt(numAntennasMmWave)));

  Ptr<MmWavePointToPointEpcHelper> epcHelper = CreateObject<MmWavePointToPointEpcHelper> ();
  mmwaveHelper->SetEpcHelper (epcHelper);

  uint8_t nMmWaveEnbNodes = 7;
  uint8_t nLteEnbNodes = 1;
  GlobalValue::GetValueByName ("ues", uintegerValue);
  uint32_t ues = uintegerValue.Get ();
  uint16_t nUeNodes = ues * nMmWaveEnbNodes;  // Changed to uint16_t for larger counts

  NS_LOG_INFO ("--- Network Topology ---");
  NS_LOG_INFO ("Bandwidth: " << bandwidth / 1e6 << " MHz");
  NS_LOG_INFO ("Center Frequency: " << centerFrequency / 1e9 << " GHz");
  NS_LOG_INFO ("ISD: " << isd << " m");
  NS_LOG_INFO ("mmWave gNBs: " << unsigned(nMmWaveEnbNodes));
  NS_LOG_INFO ("LTE eNBs: " << unsigned(nLteEnbNodes));
  NS_LOG_INFO ("UEs per gNB: " << ues);
  NS_LOG_INFO ("Total UEs: " << nUeNodes);

  // Create nodes
  Ptr<Node> pgw = epcHelper->GetPgwNode ();
  NodeContainer remoteHostContainer;
  remoteHostContainer.Create (1);
  Ptr<Node> remoteHost = remoteHostContainer.Get (0);
  InternetStackHelper internet;
  internet.Install (remoteHostContainer);

  // Internet connection
  PointToPointHelper p2ph;
  p2ph.SetDeviceAttribute ("DataRate", DataRateValue (DataRate ("100Gb/s")));
  p2ph.SetDeviceAttribute ("Mtu", UintegerValue (2500));
  p2ph.SetChannelAttribute ("Delay", TimeValue (Seconds (0.010)));
  NetDeviceContainer internetDevices = p2ph.Install (pgw, remoteHost);
  Ipv4AddressHelper ipv4h;
  ipv4h.SetBase ("1.0.0.0", "255.0.0.0");
  ipv4h.Assign (internetDevices);
  Ipv4StaticRoutingHelper ipv4RoutingHelper;
  Ptr<Ipv4StaticRouting> remoteHostStaticRouting =
      ipv4RoutingHelper.GetStaticRouting (remoteHost->GetObject<Ipv4> ());
  remoteHostStaticRouting->AddNetworkRouteTo (Ipv4Address ("7.0.0.0"), Ipv4Mask ("255.0.0.0"), 1);

  // Create network nodes
  NodeContainer ueNodes;
  NodeContainer mmWaveEnbNodes;
  NodeContainer lteEnbNodes;
  NodeContainer allEnbNodes;
  mmWaveEnbNodes.Create (nMmWaveEnbNodes);
  lteEnbNodes.Create (nLteEnbNodes);
  ueNodes.Create (nUeNodes);
  allEnbNodes.Add (lteEnbNodes);
  allEnbNodes.Add (mmWaveEnbNodes);

  // Position allocation
  Vector centerPosition = Vector (maxXAxis / 2, maxYAxis / 2, 3);
  Ptr<ListPositionAllocator> enbPositionAlloc = CreateObject<ListPositionAllocator> ();
  enbPositionAlloc->Add (centerPosition);
  enbPositionAlloc->Add (centerPosition);

  double x, y;
  double nConstellation = nMmWaveEnbNodes - 1;
  for (int8_t i = 0; i < nConstellation; ++i)
    {
      x = isd * cos ((2 * M_PI * i) / (nConstellation));
      y = isd * sin ((2 * M_PI * i) / (nConstellation));
      enbPositionAlloc->Add (Vector (centerPosition.x + x, centerPosition.y + y, 3));
    }

  MobilityHelper enbmobility;
  enbmobility.SetMobilityModel ("ns3::ConstantPositionMobilityModel");
  enbmobility.SetPositionAllocator (enbPositionAlloc);
  enbmobility.Install (allEnbNodes);

  Ptr<UniformDiscPositionAllocator> uePositionAlloc = CreateObject<UniformDiscPositionAllocator> ();
  uePositionAlloc->SetX (centerPosition.x);
  uePositionAlloc->SetY (centerPosition.y);
  uePositionAlloc->SetRho (0.7 * isd);

  // Configure UE mobility by traffic type
  uint32_t countUe;
  uint32_t numEmbbUes = (uint32_t)(ueNodes.GetN () * PercUEeMBB);
  uint32_t numUrllcUes = (uint32_t)(ueNodes.GetN () * PercUEURLLC);

  // eMBB UEs - low mobility
  for (countUe = 0; countUe < numEmbbUes; countUe++)
    {
      MobilityHelper uemobilityeMBB;
      Ptr<UniformRandomVariable> speed = CreateObject<UniformRandomVariable> ();
      speed->SetAttribute ("Min", DoubleValue (0.3));
      speed->SetAttribute ("Max", DoubleValue (3));
      uemobilityeMBB.SetMobilityModel ("ns3::RandomWalk2dOutdoorMobilityModel", "Speed",
                                       PointerValue (speed), "Bounds",
                                       RectangleValue (Rectangle (0, maxXAxis, 0, maxYAxis)));
      uemobilityeMBB.SetPositionAllocator (uePositionAlloc);
      uemobilityeMBB.Install (ueNodes.Get (countUe));
      NS_LOG_INFO ("UE " << countUe + 1 << " configured as eMBB (low mobility)");
    }

  // URLLC UEs - high mobility
  for (; countUe < numEmbbUes + numUrllcUes; countUe++)
    {
      MobilityHelper uemobilityURLLC;
      Ptr<UniformRandomVariable> speed = CreateObject<UniformRandomVariable> ();
      speed->SetAttribute ("Min", DoubleValue (5));   // 18 km/h
      speed->SetAttribute ("Max", DoubleValue (20));  // 72 km/h (vehicular)
      uemobilityURLLC.SetMobilityModel ("ns3::RandomWalk2dOutdoorMobilityModel", "Speed",
                                  PointerValue (speed), "Bounds",
                                  RectangleValue (Rectangle (0, maxXAxis, 0, maxYAxis)));
      uemobilityURLLC.SetPositionAllocator (uePositionAlloc);
      uemobilityURLLC.Install (ueNodes.Get (countUe));
      NS_LOG_INFO ("UE " << countUe + 1 << " configured as URLLC (high mobility)");
    }

  // mIoT UEs - static
  for (; countUe < ueNodes.GetN (); countUe++)
    {
      MobilityHelper uemobilitymIoT;
      uemobilitymIoT.SetPositionAllocator (uePositionAlloc);
      uemobilitymIoT.SetMobilityModel ("ns3::ConstantPositionMobilityModel");
      uemobilitymIoT.Install (ueNodes.Get (countUe));
      NS_LOG_INFO ("UE " << countUe + 1 << " configured as mIoT (static)");
    }

  // Install devices
  NetDeviceContainer lteEnbDevs = mmwaveHelper->InstallLteEnbDevice (lteEnbNodes);
  NetDeviceContainer mmWaveEnbDevs = mmwaveHelper->InstallEnbDevice (mmWaveEnbNodes);
  NetDeviceContainer mcUeDevs = mmwaveHelper->InstallMcUeDevice (ueNodes);

  internet.Install (ueNodes);
  Ipv4InterfaceContainer ueIpIface;
  ueIpIface = epcHelper->AssignUeIpv4Address (NetDeviceContainer (mcUeDevs));

  for (uint32_t u = 0; u < ueNodes.GetN (); ++u)
    {
      Ptr<Node> ueNode = ueNodes.Get (u);
      Ptr<Ipv4StaticRouting> ueStaticRouting =
          ipv4RoutingHelper.GetStaticRouting (ueNode->GetObject<Ipv4> ());
      ueStaticRouting->SetDefaultRoute (epcHelper->GetUeDefaultGatewayAddress (), 1);
    }

  mmwaveHelper->AddX2Interface (lteEnbNodes, mmWaveEnbNodes);
  mmwaveHelper->AttachToClosestEnb (mcUeDevs, mmWaveEnbDevs, lteEnbDevs);

  // ==================== APPLICATION SETUP ====================
  ApplicationContainer sinkApp;
  Ptr<LteEnbNetDevice> eNBDevice = DynamicCast<LteEnbNetDevice> (lteEnbDevs.Get (0));
  ApplicationContainer clientApp;
  PacketSinkHelper dlPacketSinkHelper ("ns3::UdpSocketFactory",
                                       InetSocketAddress (Ipv4Address::GetAny (), 1234));

  // Build data rate strings
  std::stringstream embbRateStream;
  embbRateStream << embbDataRateMbps << "Mbps";
  std::string embbRateStr = embbRateStream.str();

  std::stringstream urllcRateStream;
  urllcRateStream << urllcDataRateKbps << "kbps";
  std::string urllcRateStr = urllcRateStream.str();

  std::stringstream miotRateStream;
  miotRateStream << miotDataRateKbps << "kbps";
  std::string miotRateStr = miotRateStream.str();

  std::stringstream urllcOnTimeStream;
  urllcOnTimeStream << "ns3::ExponentialRandomVariable[Mean=" << urllcBurstMean << "]";
  std::string urllcOnTimeStr = urllcOnTimeStream.str();

  std::stringstream urllcOffTimeStream;
  urllcOffTimeStream << "ns3::ExponentialRandomVariable[Mean=" << urllcOffMean << "]";
  std::string urllcOffTimeStr = urllcOffTimeStream.str();

  NS_LOG_INFO ("--- Application Configuration ---");

  // eMBB Applications - High throughput using OnOff with high data rate
  for (countUe = 0; countUe < numEmbbUes; countUe++)
    {
      sinkApp.Add (dlPacketSinkHelper.Install (ueNodes.Get (countUe)));

      OnOffHelper embbApp ("ns3::UdpSocketFactory",
                           InetSocketAddress (ueIpIface.GetAddress (countUe), 1234));
      embbApp.SetAttribute ("PacketSize", UintegerValue (1400));  // Near MTU for efficiency
      embbApp.SetAttribute ("OnTime", StringValue ("ns3::ConstantRandomVariable[Constant=1]"));
      embbApp.SetAttribute ("OffTime", StringValue ("ns3::ConstantRandomVariable[Constant=0]"));
      embbApp.SetAttribute ("DataRate", StringValue (embbRateStr));
      clientApp.Add (embbApp.Install (remoteHost));

      NS_LOG_INFO ("eMBB UE " << countUe + 1 << ": DataRate=" << embbRateStr);

      if (qoSeMBB != -1)
        {
          Ptr<McUeNetDevice> ueDevice = DynamicCast<McUeNetDevice> (mcUeDevs.Get (countUe));
          uint64_t ueIdRnti = eNBDevice->GetRrc ()->GetRntiFromImsi (ueDevice->GetImsi ());
          NS_LOG_INFO ("[eMBB] UE IMSI " << ueDevice->GetImsi () << " initial QoS split: " << qoSeMBB);
          Simulator::Schedule (MilliSeconds (100), &LteEnbNetDevice::SetUeQoS, eNBDevice, ueIdRnti, qoSeMBB);
        }
    }

  // URLLC Applications - Frequent bursts for latency-sensitive traffic
  for (; countUe < numEmbbUes + numUrllcUes; countUe++)
    {
      sinkApp.Add (dlPacketSinkHelper.Install (ueNodes.Get (countUe)));

      OnOffHelper urllcApp ("ns3::UdpSocketFactory",
                            InetSocketAddress (ueIpIface.GetAddress (countUe), 1234));
      urllcApp.SetAttribute ("PacketSize", UintegerValue (256));  // Small packets for low latency
      urllcApp.SetAttribute ("OnTime", StringValue (urllcOnTimeStr));
      urllcApp.SetAttribute ("OffTime", StringValue (urllcOffTimeStr));
      urllcApp.SetAttribute ("DataRate", StringValue (urllcRateStr));
      clientApp.Add (urllcApp.Install (remoteHost));

      NS_LOG_INFO ("URLLC UE " << countUe + 1 << ": DataRate=" << urllcRateStr
                   << ", BurstMean=" << urllcBurstMean * 1000 << "ms");

      if (qoSURLLC != -1)
        {
          Ptr<McUeNetDevice> ueDevice = DynamicCast<McUeNetDevice> (mcUeDevs.Get (countUe));
          uint64_t ueIdRnti = eNBDevice->GetRrc ()->GetRntiFromImsi (ueDevice->GetImsi ());
          NS_LOG_INFO ("[URLLC] UE IMSI " << ueDevice->GetImsi () << " initial QoS split: " << qoSURLLC);
          Simulator::Schedule (MilliSeconds (100), &LteEnbNetDevice::SetUeQoS, eNBDevice, ueIdRnti, qoSURLLC);
        }
    }

  // mIoT Applications - Periodic low-rate transmissions
  for (; countUe < ueNodes.GetN (); countUe++)
    {
      sinkApp.Add (dlPacketSinkHelper.Install (ueNodes.Get (countUe)));

      OnOffHelper miotApp ("ns3::UdpSocketFactory",
                           InetSocketAddress (ueIpIface.GetAddress (countUe), 1234));
      miotApp.SetAttribute ("PacketSize", UintegerValue (64));  // Very small IoT packets
      miotApp.SetAttribute ("OnTime", StringValue ("ns3::ExponentialRandomVariable[Mean=0.1]"));
      miotApp.SetAttribute ("OffTime", StringValue ("ns3::ExponentialRandomVariable[Mean=0.5]"));
      miotApp.SetAttribute ("DataRate", StringValue (miotRateStr));
      clientApp.Add (miotApp.Install (remoteHost));

      NS_LOG_INFO ("mIoT UE " << countUe + 1 << ": DataRate=" << miotRateStr);

      if (qoSmIoT != -1)
        {
          Ptr<McUeNetDevice> ueDevice = DynamicCast<McUeNetDevice> (mcUeDevs.Get (countUe));
          uint64_t ueIdRnti = eNBDevice->GetRrc ()->GetRntiFromImsi (ueDevice->GetImsi ());
          NS_LOG_INFO ("[mIoT] UE IMSI " << ueDevice->GetImsi () << " initial QoS split: " << qoSmIoT);
          Simulator::Schedule (MilliSeconds (100), &LteEnbNetDevice::SetUeQoS, eNBDevice, ueIdRnti, qoSmIoT);
        }
    }

  // Start applications
  GlobalValue::GetValueByName ("simTime", doubleValue);
  double simTime = doubleValue.Get ();
  sinkApp.Start (Seconds (0));
  clientApp.Start (MilliSeconds (50));
  clientApp.Stop (Seconds (simTime));

  // Enable traces
  mmwaveHelper->EnableTraces ();
  Ptr<LteHelper> lteHelper = CreateObject<LteHelper> ();
  lteHelper->Initialize ();
  lteHelper->EnablePhyTraces ();
  lteHelper->EnableMacTraces ();

  PrintGnuplottableUeListToFile ("ues.txt");
  PrintGnuplottableEnbListToFile ("enbs.txt");

  // QoS Load Balancing logging
  std::string loadBalanceFilename = "QoSLoadBalancing.txt";
  std::ofstream header_file (loadBalanceFilename.c_str ());
  header_file << "timestamp,ueImsiComplete,QoS.CellLoadBalance.Lte,NrCellId,QoS.CellLoadBalance.Nr"
              << std::endl;
  header_file.close ();

  std::vector<Ptr<MmWaveEnbNetDevice>> mmWaveGnbs;
  Ptr<MmWaveEnbNetDevice> tempMmWaveGnb;
  for (uint32_t i = 0; i < mmWaveEnbDevs.GetN (); ++i)
    {
      tempMmWaveGnb = DynamicCast<MmWaveEnbNetDevice> (mmWaveEnbDevs.Get (i));
      if (tempMmWaveGnb)
        {
          mmWaveGnbs.push_back (tempMmWaveGnb);
        }
      else
        {
          NS_FATAL_ERROR ("DynamicCast of MmWaveEnbNetDevice failed for " << unsigned (i));
        }
    }

  // Schedule load balancing reports
  double nReports = simTime / indicationPeriodicity;
  double scheduleTime;
  for (int i = 1; i <= nReports; ++i)
    {
      scheduleTime = (i * indicationPeriodicity);
      Simulator::Schedule (Seconds (scheduleTime), &SaveLoadBalanceQoSValueToFile,
                           loadBalanceFilename, eNBDevice, mmWaveGnbs);
    }

  double offset = 0.005;
  NS_LOG_INFO ("================================================");
  NS_LOG_INFO ("Simulation time: " << simTime + offset << " seconds");
  NS_LOG_INFO ("Starting simulation...");
  NS_LOG_INFO ("================================================");

  Simulator::Stop (Seconds (simTime + offset));
  Simulator::Run ();

  NS_LOG_INFO ("Simulation completed.");
  NS_LOG_INFO (lteHelper);

  Simulator::Destroy ();
  NS_LOG_INFO ("Done.");
  return 0;
}
