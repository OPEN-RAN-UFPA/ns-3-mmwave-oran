/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2010 TELEMATICS LAB, DEE - Politecnico di Bari
 *
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
 * Author: Giuseppe Piro  <g.piro@poliba.it>
 * Author: Marco Miozzo <mmiozzo@cttc.es> : Update to FF API Architecture
 * Author: Nicola Baldo <nbaldo@cttc.es>  : Integrated with new RRC and MAC architecture
 * Author: Danilo Abrignani <danilo.abrignani@unibo.it> : Integrated with new architecture - GSoC
 * 2015 - Carrier Aggregation
 *
 * Modified by: Michele Polese <michele.polese@gmail.com>
 *          Dual Connectivity functionalities
 *          Integration of ns-O-RAN
 *
 * Modified by: Tommaso Zugno <tommasozugno@gmail.com>
 *                Integration of ns-O-RAN
 *
 * Modified by: Andrea Lacava <thecave003@gmail.com>
 *                Integration of ns-O-RAN
 *                ns-O-RAN semaphore logic
 */

#include "encode_e2apv1.hpp"

#include <ns3/abort.h>
#include <ns3/callback.h>
#include <ns3/enum.h>
#include <ns3/ff-mac-scheduler.h>
#include <ns3/ipv4-l3-protocol.h>
#include <ns3/ipv6-l3-protocol.h>
#include <ns3/llc-snap-header.h>
#include <ns3/log.h>
#include <ns3/lte-amc.h>
#include <ns3/lte-anr.h>
#include <ns3/lte-enb-component-carrier-manager.h>
#include <ns3/config.h>
#include <semaphore.h>
#include <fcntl.h>
#include <ns3/lte-enb-mac.h>
#include <ns3/lte-enb-net-device.h>
#include <ns3/lte-enb-phy.h>
#include <ns3/lte-enb-rrc.h>
#include <ns3/lte-ffr-algorithm.h>
#include <ns3/lte-handover-algorithm.h>
#include <ns3/lte-indication-message-helper.h>
#include <ns3/lte-net-device.h>
#include <ns3/lte-radio-bearer-info.h>
#include <ns3/lte-ue-net-device.h>
#include <ns3/mc-enb-pdcp.h>
#include <ns3/node.h>
#include <ns3/object-factory.h>
#include <ns3/object-map.h>
#include <ns3/packet-burst.h>
#include <ns3/packet.h>
#include <ns3/pointer.h>
#include <ns3/simulator.h>
#include <ns3/string.h>
#include <ns3/trace-source-accessor.h>
#include <ns3/uinteger.h>

#include <cstdint>
#include <fstream>
#include <numeric>
#include <sstream>

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("LteEnbNetDevice");

NS_OBJECT_ENSURE_REGISTERED(LteEnbNetDevice);

/**
 * KPM Subscription Request callback.
 * This function is triggered whenever a RIC Subscription Request for
 * the KPM RAN Function is received.
 *
 * \param pdu request message
 */
void
LteEnbNetDevice::KpmSubscriptionCallback(E2AP_PDU_t* sub_req_pdu)
{
    NS_LOG_DEBUG("Received RIC Subscription Request, cellId = " << m_cellId);

    E2Termination::RicSubscriptionRequest_rval_s params =
        m_e2term->ProcessRicSubscriptionRequest(sub_req_pdu);
    NS_LOG_DEBUG("requestorId " << +params.requestorId << ", instanceId " << +params.instanceId
                                << ", ranFuncionId " << +params.ranFuncionId << ", actionId "
                                << +params.actionId);

    if (!m_isReportingEnabled && !m_forceE2FileLogging)
    {
        BuildAndSendReportMessage(params);
        m_isReportingEnabled = true;
    }
}

std::string
LteEnbNetDevice::GetCurrentDirectory()
{
    NS_LOG_FUNCTION(this);
    int ret = std::system("pwd >path.txt");
    if (ret < 0)
    {
        NS_FATAL_ERROR("Unable to execute pwd command ");
    }

    std::string path;
    std::getline(std::ifstream("path.txt"), path);
    NS_LOG_INFO("I am running in " << path);
    return path;
}

void
LteEnbNetDevice::ReadControlFile()
{
    NS_LOG_INFO(Simulator::Now().GetMilliSeconds()
                << " Reading control file " << m_controlFilename);
    // Open the control file and read control commands
    if (m_controlFilename != "")
    {
        // --- [INÍCIO] Lógica de Semáforos (mantida como original) ---
        if (m_useSemaphores)
        {
            sem_t* metricsReadySemaphore = sem_open(m_metricsReadySemaphoreName.c_str(), 0);
            NS_ABORT_MSG_IF(metricsReadySemaphore == SEM_FAILED,
                            "Error opening metrics semaphore: " << strerror(errno));
            if (sem_post(metricsReadySemaphore) == -1)
            {
                NS_FATAL_ERROR("Error post metrics semaphore: " << strerror(errno));
            }
            sem_close(metricsReadySemaphore);

            sem_t* controlSemaphore = sem_open(m_controlSemaphoreName.c_str(), 0);
            NS_ABORT_MSG_IF(controlSemaphore == SEM_FAILED,
                            "Error opening control semaphore: " << strerror(errno));
            if (sem_wait(controlSemaphore) == -1)
            {
                NS_FATAL_ERROR("Error wait control semaphore: " << strerror(errno));
            }
            sem_close(controlSemaphore);
        }
        // --- [FIM] Lógica de Semáforos ---

        std::ifstream csv{};
        csv.open(m_controlFilename.c_str(), std::ifstream::in);
        if (!csv.is_open())
        {
            NS_LOG_WARN("Could not open control file (might be normal if Python hasn't written yet): " << m_controlFilename.c_str());
        }
        else
        {
            // --- [INÍCIO] Lógica de Leitura e Processamento Hierárquico ---
            std::string line;
            long long current_timestamp = -1; // Rastreia o timestamp mais recente lido neste ciclo
            bool has_es_action_in_batch = false; // Flag para chamar EvictUsers uma vez por lote

            while (std::getline(csv, line))
            {
                if (line.empty()) continue; // Pula linhas vazias

                std::stringstream lineStream(line);
                std::string cell;
                long long timestamp_from_file{};
                int id_from_file{};      // Pode ser ueId (TS) ou cellId+100 (ES)
                int value_from_file{};   // Pode ser targetCellId (TS) ou hoAllowed (ES)

                // Tenta fazer o parsing do formato esperado: timestamp,id,value
                if (!(std::getline(lineStream, cell, ',') &&
                      (std::stringstream(cell) >> timestamp_from_file) &&
                      std::getline(lineStream, cell, ',') &&
                      (std::stringstream(cell) >> id_from_file) &&
                      std::getline(lineStream, cell, ',') &&
                      (std::stringstream(cell) >> value_from_file)))
                {
                    NS_LOG_WARN("Skipping malformed line in control file: " << line);
                    continue;
                }

                // Apenas processa ações que são novas (timestamp > último processado)
                if (timestamp_from_file > m_lastControlTimestamp) // <<< CORRIGIDO
                {
                    // Se este é o primeiro timestamp novo que encontramos neste ciclo de leitura,
                    // atualiza o current_timestamp. Todas as ações neste ciclo devem ter o mesmo timestamp.
                    if (current_timestamp == -1) {
                        current_timestamp = timestamp_from_file;
                    } else if (timestamp_from_file != current_timestamp) {
                        NS_LOG_WARN("Control file contains mixed new timestamps (" << current_timestamp << " and " << timestamp_from_file << "). Processing only actions for ts " << current_timestamp);
                        continue;
                    }

                    NS_LOG_INFO("Processing action at ts " << timestamp_from_file << ": id=" << id_from_file << ", value=" << value_from_file);

                    if (!m_rrc) {
                         NS_LOG_ERROR("RRC component is null! Cannot process actions.");
                         continue;
                    }

                    if (id_from_file > 100)
                    {
                        // ####### AÇÃO DE ENERGY SAVING (ES) DETECTADA #######
                        uint16_t cellId = id_from_file - 100;
                        bool hoAllowed = (bool)value_from_file;

                        NS_LOG_INFO("Action identified as ES: CellID=" << cellId << ", hoAllowed=" << hoAllowed);

                        if (!m_scheduleControlMessages)
                        {
                            m_rrc->SetSecondaryCellHandoverAllowedStatus(cellId, hoAllowed);
                        }
                        else
                        {
                            Simulator::Schedule(MilliSeconds(timestamp_from_file),
                                               &LteEnbRrc::SetSecondaryCellHandoverAllowedStatus,
                                               m_rrc,
                                               cellId,
                                               hoAllowed);
                        }
                        NS_LOG_DEBUG("Executed/Scheduled SetAllowHandoverTo for CellID " << cellId << " to " << hoAllowed);
                        has_es_action_in_batch = true;
                    }
                    else
                    {
                        // ####### AÇÃO DE TRAFFIC STEERING (TS) DETECTADA #######
                        uint64_t imsi = (uint64_t)id_from_file;
                        uint16_t targetCellId = (uint16_t)value_from_file;

                        NS_LOG_INFO("Action identified as TS: IMSI=" << imsi << ", TargetCellID=" << targetCellId);

                        uint16_t rntiUe = m_rrc->GetRntiFromImsi(imsi);
                         Ptr<UeManager> ueManager = m_rrc->GetUeManager(rntiUe);

                         if (!ueManager) {
                              NS_LOG_WARN("Could not find UeManager for RNTI " << rntiUe << " (IMSI " << imsi << "). Skipping HO.");
                              continue;
                         }

                        uint16_t sourceCellId = ueManager->GetMmWaveCellId();

                        if (sourceCellId == targetCellId)
                        {
                            NS_LOG_WARN("Source CellId (" << sourceCellId << ") and Target CellId (" << targetCellId
                                      << ") are the same for IMSI " << imsi << ". Ignoring HO request.");
                            continue;
                        }

                        m_rrc->TakeUeHoControl(imsi);
                        if (!m_scheduleControlMessages)
                        {
                            // Usa GetNode() para acessar o nó e seu ID
                            Simulator::ScheduleWithContext(GetNode()->GetId(), // <<< CORRIGIDO
                                                           Seconds(0),
                                                           &LteEnbRrc::PerformHandoverToTargetCell,
                                                           m_rrc,
                                                           imsi,
                                                           targetCellId);
                        }
                        else
                        {
                             Simulator::Schedule(MilliSeconds(timestamp_from_file),
                                                &LteEnbRrc::PerformHandoverToTargetCell,
                                                m_rrc,
                                                imsi,
                                                targetCellId);
                        }
                         NS_LOG_DEBUG("Executed/Scheduled TriggerHandover for IMSI " << imsi << " to CellID " << targetCellId);

                    } // Fim do else (ação TS)
                } // Fim do if (timestamp_from_file > m_lastControlTimestamp)
                else
                {
                     NS_LOG_DEBUG("Skipping old or already processed action at timestamp " << timestamp_from_file);
                }
            } // Fim do while(getline)

            // Após ler todas as linhas novas do arquivo para o timestamp atual:
            if (has_es_action_in_batch && current_timestamp > m_lastControlTimestamp) // <<< CORRIGIDO
            {
                if (!m_rrc) {
                     NS_LOG_ERROR("RRC component is null! Cannot trigger EvictUsers.");
                } else {
                    NS_LOG_INFO("Triggering EvictUsersFromSecondaryCell for timestamp " << current_timestamp);
                    if (!m_scheduleControlMessages)
                    {
                        m_rrc->EvictUsersFromSecondaryCell();
                    }
                    else
                    {
                        Simulator::Schedule(MilliSeconds(current_timestamp + 0.001),
                                            &LteEnbRrc::EvictUsersFromSecondaryCell,
                                            m_rrc);
                    }
                }
            }

             // Atualiza o timestamp da última ação processada APENAS se encontramos ações novas
            if (current_timestamp > m_lastControlTimestamp) { // <<< CORRIGIDO
                m_lastControlTimestamp = current_timestamp; // <<< CORRIGIDO
                 NS_LOG_DEBUG("Updated m_lastControlTimestamp to " << m_lastControlTimestamp); // <<< CORRIGIDO
            }

            csv.close();

            if (!m_scheduleControlMessages)
            {
                std::ofstream csvDelete{};
                csvDelete.open(m_controlFilename.c_str(), std::ofstream::out | std::ofstream::trunc);
                if (csvDelete.is_open())
                {
                    NS_LOG_INFO("Control file flushed: " << m_controlFilename);
                    csvDelete.close();
                } else {
                    NS_LOG_ERROR("Failed to open control file for flushing: " << m_controlFilename);
                }
            }
             // --- [FIM] Lógica de Leitura e Processamento Hierárquico ---

        } // Fim do if (csv.is_open())

    } // Fim do if (m_controlFilename != "")

    // --- Lógica de Reagendamento (mantida como original) ---
    if (!m_scheduleControlMessages)
    {
        Simulator::Schedule(Seconds(m_e2Periodicity), &LteEnbNetDevice::ReadControlFile, this);
    }
}

void
LteEnbNetDevice::SetUeQoS(uint16_t ueId, double percentage)
{
    auto ueManager = m_rrc->GetUeManager(ueId);
    auto drbMap = ueManager->GetDrbMap();
    for (auto drb : drbMap)
    {
        auto dataBearer = drb.second;
        Ptr<McEnbPdcp> pdcp = DynamicCast<McEnbPdcp>(dataBearer->m_pdcp);
        if (pdcp)
        {
            NS_LOG_INFO(Simulator::Now().GetMilliSeconds()
                        << ": About to set pecentage " << percentage
                        << " on UE connectes to eNB with RNTI " << ueId);
            pdcp->SetAttribute("perPckToLTE", DoubleValue(percentage));
        }
        else
        {
            NS_LOG_WARN("pdcp not found");
        }
    }
}

double
LteEnbNetDevice::GetUeQoS(uint16_t rnti)
{
    auto ueManager = m_rrc->GetUeManager(rnti);
    auto drbMap = ueManager->GetDrbMap();
    DoubleValue percentage;
    double ueQoS;
    for (auto drb : drbMap)
    {
        auto dataBearer = drb.second;
        Ptr<McEnbPdcp> pdcp = DynamicCast<McEnbPdcp>(dataBearer->m_pdcp);
        if (pdcp)
        {
            pdcp->GetAttribute("perPckToLTE", percentage);
            ueQoS = percentage.Get();
            return ueQoS;
        }
        else
        {
            NS_LOG_UNCOND("pdcp not found");
        }
    }
    return -1.0; // This should never happen
}

double
LteEnbNetDevice::GetUeQoS(uint64_t imsi)
{
    return GetUeQoS(m_rrc->GetRntiFromImsi(imsi));
}

void
LteEnbNetDevice::ControlMessageReceivedCallback(E2AP_PDU_t* sub_req_pdu)
{
    NS_LOG_DEBUG("LteEnbNetDevice::ControlMessageReceivedCallback: Received RIC Control Message");

    Ptr<RicControlMessage> controlMessage = Create<RicControlMessage>(sub_req_pdu);
    NS_LOG_INFO("After RicControlMessage::RicControlMessage constructor");
    NS_LOG_INFO("Request type " << controlMessage->m_requestType);
    switch (controlMessage->m_requestType)
    {
    case RicControlMessage::ControlMessageRequestIdType::TS: {
        NS_LOG_INFO("TS, do the handover");
        // do handover
        Ptr<OctetString> imsiString =
            Create<OctetString>((void*)controlMessage->m_e2SmRcControlHeaderFormat1->ueId.buf,
                                controlMessage->m_e2SmRcControlHeaderFormat1->ueId.size);
        char* end;

        uint64_t imsi = std::strtoull(imsiString->DecodeContent().c_str(), &end, 10);
        uint16_t targetCellId = std::stoi(controlMessage->GetSecondaryCellIdHO());
        NS_LOG_INFO("Imsi Decoded: " << imsi);
        NS_LOG_INFO("Target Cell id " << targetCellId);
        m_rrc->TakeUeHoControl(imsi);
        if (!m_forceE2FileLogging)
        {
            Simulator::ScheduleWithContext(1,
                                           Seconds(0),
                                           &LteEnbRrc::PerformHandoverToTargetCell,
                                           m_rrc,
                                           imsi,
                                           targetCellId);
        }
        else
        {
            Simulator::Schedule(Seconds(0),
                                &LteEnbRrc::PerformHandoverToTargetCell,
                                m_rrc,
                                imsi,
                                targetCellId);
        }
        break;
    }
    case RicControlMessage::ControlMessageRequestIdType::QoS: {
        // use SetUeQoS()
        NS_FATAL_ERROR("For QoS use file-based control.");
        break;
    }
    default: {
        NS_LOG_ERROR("Unrecognized id type of Ric Control Message");
        break;
    }
    }
}

TypeId
LteEnbNetDevice::GetTypeId(void)
{
    static TypeId tid =
        TypeId("ns3::LteEnbNetDevice")
            .SetParent<LteNetDevice>()
            .AddConstructor<LteEnbNetDevice>()
            .AddAttribute("LteEnbRrc",
                          "The RRC associated to this EnbNetDevice",
                          PointerValue(),
                          MakePointerAccessor(&LteEnbNetDevice::m_rrc),
                          MakePointerChecker<LteEnbRrc>())
            .AddAttribute("LteHandoverAlgorithm",
                          "The handover algorithm associated to this EnbNetDevice",
                          PointerValue(),
                          MakePointerAccessor(&LteEnbNetDevice::m_handoverAlgorithm),
                          MakePointerChecker<LteHandoverAlgorithm>())
            .AddAttribute(
                "LteAnr",
                "The automatic neighbour relation function associated to this EnbNetDevice",
                PointerValue(),
                MakePointerAccessor(&LteEnbNetDevice::m_anr),
                MakePointerChecker<LteAnr>())
            .AddAttribute("LteFfrAlgorithm",
                          "The FFR algorithm associated to this EnbNetDevice",
                          PointerValue(),
                          MakePointerAccessor(&LteEnbNetDevice::m_ffrAlgorithm),
                          MakePointerChecker<LteFfrAlgorithm>())
            .AddAttribute("LteEnbComponentCarrierManager",
                          "The RRC associated to this EnbNetDevice",
                          PointerValue(),
                          MakePointerAccessor(&LteEnbNetDevice::m_componentCarrierManager),
                          MakePointerChecker<LteEnbComponentCarrierManager>())
            .AddAttribute("ComponentCarrierMap",
                          "List of component carriers.",
                          ObjectMapValue(),
                          MakeObjectMapAccessor(&LteEnbNetDevice::m_ccMap),
                          MakeObjectMapChecker<ComponentCarrierEnb>())
            .AddAttribute(
                "UlBandwidth",
                "Uplink Transmission Bandwidth Configuration in number of Resource Blocks",
                UintegerValue(100),
                MakeUintegerAccessor(&LteEnbNetDevice::SetUlBandwidth,
                                     &LteEnbNetDevice::GetUlBandwidth),
                MakeUintegerChecker<uint8_t>())
            .AddAttribute(
                "DlBandwidth",
                "Downlink Transmission Bandwidth Configuration in number of Resource Blocks",
                UintegerValue(100),
                MakeUintegerAccessor(&LteEnbNetDevice::SetDlBandwidth,
                                     &LteEnbNetDevice::GetDlBandwidth),
                MakeUintegerChecker<uint8_t>())
            .AddAttribute("CellId",
                          "Cell Identifier",
                          UintegerValue(0),
                          MakeUintegerAccessor(&LteEnbNetDevice::m_cellId),
                          MakeUintegerChecker<uint16_t>())
            .AddAttribute("DlEarfcn",
                          "Downlink E-UTRA Absolute Radio Frequency Channel Number (EARFCN) "
                          "as per 3GPP 36.101 Section 5.7.3. ",
                          UintegerValue(100),
                          MakeUintegerAccessor(&LteEnbNetDevice::m_dlEarfcn),
                          MakeUintegerChecker<uint32_t>(0, 262143))
            .AddAttribute("UlEarfcn",
                          "Uplink E-UTRA Absolute Radio Frequency Channel Number (EARFCN) "
                          "as per 3GPP 36.101 Section 5.7.3. ",
                          UintegerValue(18100),
                          MakeUintegerAccessor(&LteEnbNetDevice::m_ulEarfcn),
                          MakeUintegerChecker<uint32_t>(0, 262143))
            .AddAttribute(
                "CsgId",
                "The Closed Subscriber Group (CSG) identity that this eNodeB belongs to",
                UintegerValue(0),
                MakeUintegerAccessor(&LteEnbNetDevice::SetCsgId, &LteEnbNetDevice::GetCsgId),
                MakeUintegerChecker<uint32_t>())
            .AddAttribute(
                "CsgIndication",
                "If true, only UEs which are members of the CSG (i.e. same CSG ID) "
                "can gain access to the eNodeB, therefore enforcing closed access mode. "
                "Otherwise, the eNodeB operates as a non-CSG cell and implements open access mode.",
                BooleanValue(false),
                MakeBooleanAccessor(&LteEnbNetDevice::SetCsgIndication,
                                    &LteEnbNetDevice::GetCsgIndication),
                MakeBooleanChecker())
            .AddAttribute("E2Termination",
                          "The E2 termination object associated to this node",
                          PointerValue(),
                          MakePointerAccessor(&LteEnbNetDevice::SetE2Termination,
                                              &LteEnbNetDevice::GetE2Termination),
                          MakePointerChecker<E2Termination>())
            .AddAttribute("E2PdcpCalculator",
                          "The PDCP calculator object for E2 reporting",
                          PointerValue(),
                          MakePointerAccessor(&LteEnbNetDevice::m_e2PdcpStatsCalculator),
                          MakePointerChecker<mmwave::MmWaveBearerStatsCalculator>())
            .AddAttribute("E2RlcCalculator",
                          "The RLC calculator object for E2 reporting",
                          PointerValue(),
                          MakePointerAccessor(&LteEnbNetDevice::m_e2RlcStatsCalculator),
                          MakePointerChecker<mmwave::MmWaveBearerStatsCalculator>())
            .AddAttribute("E2Periodicity",
                          "Periodicity of E2 reporting (value in seconds)",
                          DoubleValue(0.1),
                          MakeDoubleAccessor(&LteEnbNetDevice::m_e2Periodicity),
                          MakeDoubleChecker<double>())
            .AddAttribute("EnableCuUpReport",
                          "If true, send CuUpReport",
                          BooleanValue(true),
                          MakeBooleanAccessor(&LteEnbNetDevice::m_sendCuUp),
                          MakeBooleanChecker())
            .AddAttribute("EnableCuCpReport",
                          "If true, send CuCpReport",
                          BooleanValue(true),
                          MakeBooleanAccessor(&LteEnbNetDevice::m_sendCuCp),
                          MakeBooleanChecker())
            .AddAttribute("ReducedPmValues",
                          "If true, send only a subset of pmValues",
                          BooleanValue(false),
                          MakeBooleanAccessor(&LteEnbNetDevice::m_reducedPmValues),
                          MakeBooleanChecker())
            .AddAttribute("EnableE2FileLogging",
                          "If true, force E2 indication generation and write E2 fields in csv file",
                          BooleanValue(false),
                          MakeBooleanAccessor(&LteEnbNetDevice::m_forceE2FileLogging),
                          MakeBooleanChecker())
            .AddAttribute(
                "ControlFileName",
                "Filename for the stand alone control mode. The file is deleted after every read."
                "Format should correspond to the particular use case:\n"
                "TS: Contains multiple lines with ts, imsi, targetCellId\n"
                "QoS: Contains multiple lines with ts, imsi, qos\n"
                "EE: Contains multiple lines with ts, cellId, action",
                StringValue(""),
                MakeStringAccessor(&LteEnbNetDevice::m_controlFilename),
                MakeStringChecker())
            .AddAttribute("ScheduleControlMessages",
                          "If true, the control action will be scheduled at the given timestamp in "
                          "milliseconds.\n"
                          "If false, the control action will be executed when parsed.\n",
                          BooleanValue(false),
                          MakeBooleanAccessor(&LteEnbNetDevice::m_scheduleControlMessages),
                          MakeBooleanChecker())
            .AddAttribute("UseSemaphores",
                          "If true, we use the semaphores to handle communication with an external "
                          "environment",
                          BooleanValue(false),
                          MakeBooleanAccessor(&LteEnbNetDevice::m_useSemaphores),
                          MakeBooleanChecker());
    return tid;
}

LteEnbNetDevice::LteEnbNetDevice()
    : m_isConstructed(false),
      m_isConfigured(false),
      m_anr(0),
      m_componentCarrierManager(0),
      m_isReportingEnabled(false),
      m_reducedPmValues(false),
      m_forceE2FileLogging(false),
      m_useSemaphores(false),
      m_cuUpFileName(),
      m_cuCpFileName(),
      m_lastControlTimestamp(-1)
{
    NS_LOG_FUNCTION(this);
}

LteEnbNetDevice::~LteEnbNetDevice(void)
{
    NS_LOG_FUNCTION(this);
}

void
LteEnbNetDevice::DoDispose()
{
    NS_LOG_FUNCTION(this);

    m_rrc->Dispose();
    m_rrc = 0;

    m_handoverAlgorithm->Dispose();
    m_handoverAlgorithm = 0;

    if (m_anr)
    {
        m_anr->Dispose();
        m_anr = 0;
    }
    m_componentCarrierManager->Dispose();
    m_componentCarrierManager = 0;
    // ComponentCarrierEnb::DoDispose() will call DoDispose
    // of its PHY, MAC, FFR and scheduler instance
    for (uint32_t i = 0; i < m_ccMap.size(); i++)
    {
        m_ccMap.at(i)->Dispose();
        m_ccMap.at(i) = 0;
    }

    LteNetDevice::DoDispose();
}

Ptr<LteEnbMac>
LteEnbNetDevice::GetMac() const
{
    return m_ccMap.at(0)->GetMac();
}

Ptr<LteEnbPhy>
LteEnbNetDevice::GetPhy() const
{
    return m_ccMap.at(0)->GetPhy();
}

Ptr<LteEnbMac>
LteEnbNetDevice::GetMac(uint8_t index)
{
    return m_ccMap.at(index)->GetMac();
}

Ptr<LteEnbPhy>
LteEnbNetDevice::GetPhy(uint8_t index)
{
    return m_ccMap.at(index)->GetPhy();
}

Ptr<LteEnbRrc>
LteEnbNetDevice::GetRrc() const
{
    return m_rrc;
}

Ptr<LteEnbComponentCarrierManager>
LteEnbNetDevice::GetComponentCarrierManager() const
{
    return m_componentCarrierManager;
}

uint16_t
LteEnbNetDevice::GetCellId() const
{
    return m_cellId;
}

bool
LteEnbNetDevice::HasCellId(uint16_t cellId) const
{
    for (auto& it : m_ccMap)
    {
        if (it.second->GetCellId() == cellId)
        {
            return true;
        }
    }
    return false;
}

uint8_t
LteEnbNetDevice::GetUlBandwidth() const
{
    return m_ulBandwidth;
}

void
LteEnbNetDevice::SetUlBandwidth(uint8_t bw)
{
    NS_LOG_FUNCTION(this << uint16_t(bw));
    switch (bw)
    {
    case 6:
    case 15:
    case 25:
    case 50:
    case 75:
    case 100:
        m_ulBandwidth = bw;
        break;

    default:
        NS_FATAL_ERROR("invalid bandwidth value " << (uint16_t)bw);
        break;
    }
}

uint8_t
LteEnbNetDevice::GetDlBandwidth() const
{
    return m_dlBandwidth;
}

void
LteEnbNetDevice::SetDlBandwidth(uint8_t bw)
{
    NS_LOG_FUNCTION(this << uint16_t(bw));
    switch (bw)
    {
    case 6:
    case 15:
    case 25:
    case 50:
    case 75:
    case 100:
        m_dlBandwidth = bw;
        break;

    default:
        NS_FATAL_ERROR("invalid bandwidth value " << (uint16_t)bw);
        break;
    }
}

uint32_t
LteEnbNetDevice::GetDlEarfcn() const
{
    return m_dlEarfcn;
}

void
LteEnbNetDevice::SetDlEarfcn(uint32_t earfcn)
{
    NS_LOG_FUNCTION(this << earfcn);
    m_dlEarfcn = earfcn;
}

uint32_t
LteEnbNetDevice::GetUlEarfcn() const
{
    return m_ulEarfcn;
}

void
LteEnbNetDevice::SetUlEarfcn(uint32_t earfcn)
{
    NS_LOG_FUNCTION(this << earfcn);
    m_ulEarfcn = earfcn;
}

uint32_t
LteEnbNetDevice::GetCsgId() const
{
    return m_csgId;
}

void
LteEnbNetDevice::SetCsgId(uint32_t csgId)
{
    NS_LOG_FUNCTION(this << csgId);
    m_csgId = csgId;
    UpdateConfig(); // propagate the change to RRC level
}

bool
LteEnbNetDevice::GetCsgIndication() const
{
    return m_csgIndication;
}

void
LteEnbNetDevice::SetCsgIndication(bool csgIndication)
{
    NS_LOG_FUNCTION(this << csgIndication);
    m_csgIndication = csgIndication;
    UpdateConfig(); // propagate the change to RRC level
}

std::map<uint8_t, Ptr<ComponentCarrierEnb>>
LteEnbNetDevice::GetCcMap()
{
    return m_ccMap;
}

void
LteEnbNetDevice::SetCcMap(std::map<uint8_t, Ptr<ComponentCarrierEnb>> ccm)
{
    NS_ASSERT_MSG(!m_isConfigured, "attempt to set CC map after configuration");
    m_ccMap = ccm;
}

void
LteEnbNetDevice::DoInitialize(void)
{
    NS_LOG_FUNCTION(this);
    m_isConstructed = true;
    UpdateConfig();
    std::map<uint8_t, Ptr<ComponentCarrierEnb>>::iterator it;
    for (it = m_ccMap.begin(); it != m_ccMap.end(); ++it)
    {
        it->second->Initialize();
    }
    m_rrc->Initialize();
    m_componentCarrierManager->Initialize();
    m_handoverAlgorithm->Initialize();

    if (m_anr)
    {
        m_anr->Initialize();
    }

    m_ffrAlgorithm->Initialize();
}

void
LteEnbNetDevice::ReportCurrentCellRsrpSinr(Ptr<LteEnbNetDevice> netDev,
                                           std::string context,
                                           uint16_t cellId,
                                           uint16_t rnti,
                                           double rsrp,
                                           double sinr,
                                           uint8_t componentCarrierId)
{
    uint64_t imsi = netDev->GetRrc()->GetImsiFromRnti(rnti);
    // NS_LOG_UNCOND ("RegisterNewSinrReadingCallback rnti" << std::to_string(rnti)<< " imsi "<<
    // std::to_string(imsi));
    netDev->RegisterNewSinrReading(imsi, cellId, sinr);
}

void
LteEnbNetDevice::RegisterNewSinrReading(uint64_t imsi, uint16_t cellId, long double sinr)
{
    if (!m_sendCuCp)
    {
        return;
    }

    // Create key
    ImsiCellIdPair_t imsiCid{imsi, cellId};

    // We only need to save the last value, so we do not care about overwriting or not
    m_l3sinrMap[imsiCid] = sinr;

    NS_LOG_LOGIC(Simulator::Now().GetSeconds()
                 << " enbdev " << m_cellId << " UE " << imsi << " report for " << cellId << " SINR "
                 << m_l3sinrMap[imsiCid]);
}

bool
LteEnbNetDevice::Send(Ptr<Packet> packet, const Address& dest, uint16_t protocolNumber)
{
    NS_LOG_FUNCTION(this << packet << dest << protocolNumber);
    NS_ABORT_MSG_IF(protocolNumber != Ipv4L3Protocol::PROT_NUMBER &&
                        protocolNumber != Ipv6L3Protocol::PROT_NUMBER,
                    "unsupported protocol " << protocolNumber
                                            << ", only IPv4 and IPv6 are supported");
    return m_rrc->SendData(packet);
}

void
LteEnbNetDevice::UpdateConfig(void)
{
    NS_LOG_FUNCTION(this);

    if (m_isConstructed)
    {
        if (!m_isConfigured)
        {
            NS_LOG_LOGIC(this << " Configure cell " << m_cellId);
            // we have to make sure that this function is called only once
            NS_ASSERT(!m_ccMap.empty());
            m_rrc->ConfigureCell(m_ccMap);
            m_isConfigured = true;
        }

        NS_LOG_LOGIC(this << " Updating SIB1 of cell " << m_cellId << " with CSG ID " << m_csgId
                          << " and CSG indication " << m_csgIndication);
        m_rrc->SetCsgId(m_csgId, m_csgIndication);

        if (m_e2term)
        {
            NS_LOG_DEBUG("E2sim start in cell " << m_cellId << " force CSV logging "
                                                << m_forceE2FileLogging);

            if (!m_forceE2FileLogging)
            {
                Simulator::Schedule(MicroSeconds(0), &E2Termination::Start, m_e2term);
            }
            else
            { // give some time for the simulation to start, TODO check value
                m_cuUpFileName = "cu-up-cell-" + std::to_string(m_cellId) + ".txt";
                std::ofstream csv{};
                csv.open(m_cuUpFileName.c_str());
                csv << "timestamp,ueImsiComplete,DRB.PdcpSduDelayDl(cellAverageLatency),"
                       "m_pDCPBytesUL(0),"
                       "m_pDCPBytesDL(cellDlTxVolume),"
                       "DRB.PdcpSduVolumeDl_Filter.UEID(txBytes),"
                       "Tot.PdcpSduNbrDl.UEID(txDlPackets),"
                       "DRB.PdcpSduBitRateDl.UEID(pdcpThroughput),"
                       "DRB.PdcpSduDelayDl.UEID(pdcpLatency),"
                       "txPdcpPduLteRlc,txPdcpPduBytesLteRlc,"
                       "QosFlow.PdcpPduVolumeDL_Filter.UEID(txPdcpPduBytesNrRlc),"
                       "DRB.PdcpPduNbrDl.Qos.UEID(txPdcpPduNrRlc)\n";
                csv.close();

                m_cuCpFileName = "cu-cp-cell-" + std::to_string(m_cellId) + ".txt";
                csv.open(m_cuCpFileName.c_str());
                csv << "timestamp,ueImsiComplete,numActiveUes,RRC.ConnMean,DRB.EstabSucc.5QI.UEID "
                       "(numDrb),"
                       "DRB.RelActNbr.5QI.UEID (0),eNB id,sameCellSinr,"
                       "sameCellSinr 3gpp encoded\n";
                csv.close();
                Simulator::Schedule(MicroSeconds(500),
                                    &LteEnbNetDevice::BuildAndSendReportMessage,
                                    this,
                                    E2Termination::RicSubscriptionRequest_rval_s{});
                NS_LOG_INFO("About to schedule control file function");
                if (m_useSemaphores)
                {
                    std::string currentDirectory = GetCurrentDirectory();
                    // Extract folder name
                    size_t lastDelimiterPos = currentDirectory.find_last_of('/');
                    m_metricsReadySemaphoreName = "/sem_metrics_";
                    m_controlSemaphoreName = "/sem_control_";
                    if (lastDelimiterPos != std::string::npos)
                    {
                        m_metricsReadySemaphoreName +=
                            currentDirectory.substr(lastDelimiterPos + 1);
                        m_controlSemaphoreName += currentDirectory.substr(lastDelimiterPos + 1);
                    }
                    else
                    {
                        m_metricsReadySemaphoreName += currentDirectory;
                        m_controlSemaphoreName += currentDirectory;
                    }

                    NS_LOG_INFO("Name of semaphore for metrics is "
                                << m_metricsReadySemaphoreName.c_str());
                    NS_LOG_INFO("Name of semaphore for control is "
                                << m_controlSemaphoreName.c_str());

                    // Ensure semaphores are created
                    int initial_count = 0;
                    sem_t* metricsReadySemaphore = sem_open(m_metricsReadySemaphoreName.c_str(),
                                                            O_CREAT,
                                                            S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP,
                                                            initial_count);
                    NS_ABORT_MSG_IF(
                        metricsReadySemaphore == SEM_FAILED,
                        "Error in opening the metrics semaphore, errno: " << strerror(errno));
                    sem_close(metricsReadySemaphore);
                    metricsReadySemaphore = nullptr;

                    sem_t* controlSemaphore = sem_open(m_controlSemaphoreName.c_str(),
                                                       O_CREAT,
                                                       S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP,
                                                       initial_count);
                    NS_ABORT_MSG_IF(
                        controlSemaphore == SEM_FAILED,
                        "Error in opening the control semaphore, errno: " << strerror(errno));
                    sem_close(controlSemaphore);
                    controlSemaphore = nullptr;

                    // This is needed when the output of the simulation and the control file are
                    // saved in a different folder from the ns-3 one
                    m_controlFilename = currentDirectory + '/' + m_controlFilename;
                    NS_LOG_INFO("The filename of the control file is " << m_controlFilename);
                }

                // Set the first Control Action to happen after 5 ms to the first m_e2Periodicity
                Time e2ControlPeriodicity = Seconds(m_e2Periodicity) + MilliSeconds(5);
                Simulator::Schedule(e2ControlPeriodicity, &LteEnbNetDevice::ReadControlFile, this);
            }

            // Regardless the offline or online mode for reporting the files, we always want to
            // register the mean of RRC UEs
            Simulator::Schedule(MilliSeconds(10), &LteEnbNetDevice::SaveActiveUes, this);
        }
    }
    else
    {
        /*
         * Lower layers are not ready yet, so do nothing now and expect
         * ``DoInitialize`` to re-invoke this function.
         */
    }
}

Ptr<E2Termination>
LteEnbNetDevice::GetE2Termination() const
{
    return m_e2term;
}

void
LteEnbNetDevice::SetE2Termination(Ptr<E2Termination> e2term)
{
    m_e2term = e2term;

    NS_LOG_DEBUG("Register E2SM");

    if (!m_forceE2FileLogging)
    {
        Ptr<KpmFunctionDescription> kpmFd = Create<KpmFunctionDescription>();
        e2term->RegisterKpmCallbackToE2Sm(
            200,
            kpmFd,
            std::bind(&LteEnbNetDevice::KpmSubscriptionCallback, this, std::placeholders::_1));

        Ptr<RicControlFunctionDescription> ricCtrlFd = Create<RicControlFunctionDescription>();
        e2term->RegisterSmCallbackToE2Sm(300,
                                         ricCtrlFd,
                                         std::bind(&LteEnbNetDevice::ControlMessageReceivedCallback,
                                                   this,
                                                   std::placeholders::_1));
    }
}

std::string
LteEnbNetDevice::GetImsiString(uint64_t imsi)
{
    std::string ueImsi = std::to_string(imsi);
    std::string ueImsiComplete{};
    if (ueImsi.length() == 1)
    {
        ueImsiComplete = "0000" + ueImsi;
    }
    else if (ueImsi.length() == 2)
    {
        ueImsiComplete = "000" + ueImsi;
    }
    else
    {
        ueImsiComplete = "00" + ueImsi;
    }
    return ueImsiComplete;
}

Ptr<KpmIndicationHeader>
LteEnbNetDevice::BuildRicIndicationHeader(std::string plmId, std::string gnbId, uint16_t nrCellId)
{
    if (!m_forceE2FileLogging)
    {
        KpmIndicationHeader::KpmRicIndicationHeaderValues headerValues;
        headerValues.m_plmId = plmId;
        headerValues.m_gnbId = gnbId;
        headerValues.m_nrCellId = nrCellId;
        uint64_t timestamp = m_startTime + Simulator::Now().GetMilliSeconds();
        NS_LOG_DEBUG("NR plmid " << plmId << " gnbId " << gnbId << " nrCellId " << nrCellId);
        NS_LOG_DEBUG("Timestamp " << timestamp);
        headerValues.m_timestamp = timestamp;
        Ptr<KpmIndicationHeader> header =
            Create<KpmIndicationHeader>(KpmIndicationHeader::GlobalE2nodeType::eNB, headerValues);

        return header;
    }
    else
    {
        return nullptr;
    }
}

Ptr<KpmIndicationMessage>
LteEnbNetDevice::BuildRicIndicationMessageCuUp(std::string plmId)
{
    Ptr<LteIndicationMessageHelper> indicationMessageHelper =
        Create<LteIndicationMessageHelper>(IndicationMessageHelper::IndicationMessageType::CuUp,
                                           m_forceE2FileLogging,
                                           m_reducedPmValues);

    // get <rnti, UeManager> map of connected UEs
    auto ueMap = m_rrc->GetUeMap();
    // gNB-wide PDCP volume in downlink
    uint64_t cellDlTxVolume = 0; // in kbit

    // sum of the per-user average latency
    double perUserAverageLatencySum = 0;

    std::unordered_map<uint64_t, std::string> uePmString{};

    for (auto ue : ueMap)
    {
        uint64_t imsi = ue.second->GetImsi();
        std::string ueImsiComplete = GetImsiString(imsi);

        uint64_t txDlPackets =
            m_e2PdcpStatsCalculator->GetDlTxPackets(imsi, 3); // LCID 3 is used for data
        uint64_t txBytes =
            std::round (m_e2PdcpStatsCalculator->GetDlTxData(imsi, 3) * 8 / 1e3); // in kbit, not byte
        cellDlTxVolume += txBytes;

        uint64_t txPdcpPduLteRlc = 0;
        uint64_t txPdcpPduBytesLteRlc = 0;
        auto drbMap = ue.second->GetDrbMap();
        NS_LOG_INFO("About to get into the DRB map for ue " << std::to_string(imsi) << " size " << drbMap.size());

        uint64_t txE2DlPduRlc = m_e2RlcStatsCalculator->GetDlTxPackets(imsi, 3);
        uint64_t txE2DlBytesRlc = std::round( m_e2RlcStatsCalculator->GetDlTxData(imsi, 3) * 8 / 1e3); // in kbit, not byte

        NS_LOG_INFO("ue id " << std::to_string(imsi) << " txE2DlPduRlc " << std::to_string(txE2DlPduRlc));
        NS_LOG_INFO("ue id " << std::to_string(imsi) << " txE2DlBytesRlc " << std::to_string(txE2DlBytesRlc));

        for (auto drb : drbMap)
        {
            txPdcpPduLteRlc += drb.second->m_rlc->GetTxPacketsInReportingPeriod();
            txPdcpPduBytesLteRlc += drb.second->m_rlc->GetTxBytesInReportingPeriod();
            NS_LOG_INFO("ue id " << std::to_string(imsi) << " txPdcpPduLteRlc " << std::to_string(txPdcpPduLteRlc));
            NS_LOG_INFO("ue id " << std::to_string(imsi) << " txPdcpPduBytesLteRlc " << std::to_string(txPdcpPduBytesLteRlc));
            drb.second->m_rlc->ResetRlcCounters();
        }

        txPdcpPduBytesLteRlc = std::round(txPdcpPduBytesLteRlc * 8 / 1e3); // in kbit, not byte
        NS_LOG_INFO("ue id " << std::to_string(imsi) <<" final txPdcpPduBytesLteRlc after *= 8 / 1e3: " << std::to_string(txPdcpPduBytesLteRlc));

        NS_LOG_INFO("txDlPackets (" << txDlPackets << ") vs txPdcpPduLteRlc ("
                                  << txPdcpPduLteRlc << ") and txBytes (" << txBytes << ") vs txPdcpPduBytesLteRlc (" << txPdcpPduBytesLteRlc
                                  << ")\n");

        /**
         * The assert below can be triggered in one cell scenario if the UE has not connected yet to
         * the RLC and the traffic from application layer started. To fix this move the start of the
         * traffic later. If there are more than one LteNetDevice, the handover between cel handover
         * where the reported NR RLC packets and bytes exceed the reported cell values from the
         * PDCP. This occurs because RLC buffers are transferred between cells during the handover,
         * and if this happens during the reset of an E2 indication periodicity, mismatches can
         * arise.
         */

        NS_ASSERT_MSG(txBytes >= txPdcpPduBytesLteRlc && txDlPackets >= txPdcpPduLteRlc,
                      "txBytes (" << txBytes << ") < txPdcpPduBytesLteRlc (" << txPdcpPduBytesLteRlc
                                  << ") txDlPackets (" << txDlPackets << ") < txPdcpPduLteRlc ("
                                  << txPdcpPduLteRlc << ")");

        uint64_t txPdcpPduNrRlc = std::max(static_cast<uint64_t>(0), txDlPackets - txPdcpPduLteRlc);
        double txPdcpPduBytesNrRlc = std::max(static_cast<uint64_t>(0), txBytes - txPdcpPduBytesLteRlc);

        NS_LOG_INFO("ue id " << std::to_string(imsi)
                    << " According to LTE, txPdcpPduNrRlc " << std::to_string(txPdcpPduNrRlc)
                    <<  " txPdcpPduBytesNrRlc " << std::to_string(txPdcpPduBytesNrRlc)
        );

        double pdcpLatency = m_e2PdcpStatsCalculator->GetDlDelay(imsi, 3) / 1e5; // unit: x 0.1 ms
        perUserAverageLatencySum += pdcpLatency;

        double pdcpThroughput = txBytes / m_e2Periodicity; // unit kbps

        NS_LOG_DEBUG(Simulator::Now().GetSeconds()
                     << " " << std::to_string(m_cellId) << " cell, connected UE with IMSI " << std::to_string(imsi)
                     << " ueImsiString " << ueImsiComplete << " txDlPackets " << txDlPackets
                     << " txPdcpPduNrRlc " << txPdcpPduNrRlc
                     << " txBytes " << txBytes
                     << " txPdcpPduLteRlc " << txPdcpPduLteRlc
                     << " txPdcpPduBytesLteRlc " << txPdcpPduBytesLteRlc
                     << " txPdcpPduBytesNrRlc " << txPdcpPduBytesNrRlc
                     << " pdcpLatency " << pdcpLatency
                     << " pdcpThroughput " << pdcpThroughput);

        m_e2PdcpStatsCalculator->ResetResultsForImsiLcid(imsi, 3);

        if (!indicationMessageHelper->IsOffline())
        {
            indicationMessageHelper->AddCuUpUePmItem(ueImsiComplete,
                                                     txBytes,
                                                     txDlPackets,
                                                     pdcpThroughput,
                                                     pdcpLatency);
        }

        // We include here the NR RLC PDU and Bytes even though it is LTE since they traces are not working properly
        // To remove them from the report, remove last two elements keeping commas
        uePmString.insert(std::make_pair(
            imsi,
            std::to_string(txBytes) + "," + std::to_string(txDlPackets) + "," +
                std::to_string(pdcpThroughput) + "," + std::to_string(pdcpLatency) + "," +
                std::to_string(txPdcpPduLteRlc) + "," + std::to_string(txPdcpPduBytesLteRlc)
                + "," + std::to_string(txPdcpPduBytesNrRlc)+ "," + std::to_string(txPdcpPduNrRlc)));
    }

    // get average cell latency
    double cellAverageLatency = 0;
    if (!ueMap.empty())
    {
        cellAverageLatency = perUserAverageLatencySum / ueMap.size();
    }

    NS_LOG_DEBUG(Simulator::Now().GetSeconds()
                 << " " << std::to_string(m_cellId) << " cell, connected UEs number "
                 << ueMap.size() << " cellAverageLatency " << cellAverageLatency);

    if (!indicationMessageHelper->IsOffline())
    {
        indicationMessageHelper->AddCuUpCellPmItem(cellAverageLatency);
    }

    // PDCP volume for the whole cell
    if (!indicationMessageHelper->IsOffline())
    {
        // pDCPBytesUL = 0 since it is not supported from the simulator
        indicationMessageHelper->FillCuUpValues(plmId, 0, cellDlTxVolume);
    }

    NS_LOG_DEBUG(Simulator::Now().GetSeconds()
                 << " " << std::to_string(m_cellId) << " cell volume Lte " << cellDlTxVolume);

    if (m_forceE2FileLogging)
    {
        std::ofstream csv{};
        csv.open(m_cuUpFileName.c_str(), std::ios_base::app);
        if (!csv.is_open())
        {
            NS_FATAL_ERROR("Can't open file " << m_cuUpFileName.c_str());
        }

        uint64_t timestamp = m_startTime + Simulator::Now().GetMilliSeconds();

        // the string is timestamp,ueImsiComplete,DRB.PdcpSduDelayDl(cellAverageLatency),
        // m_pDCPBytesUL(0),m_pDCPBytesDL(cellDlTxVolume),DRB.PdcpSduVolumeDl_Filter.UEID
        // (txBytes),Tot.PdcpSduNbrDl.UEID (txDlPackets),
        // DRB.PdcpSduBitRateDl.UEID(pdcpThroughput),DRB.PdcpSduDelayDl.UEID(pdcpLatency),
        // txPdcpPduLteRlc,txPdcpPduBytesLteRlc,
        // QosFlow.PdcpPduVolumeDL_Filter.UEID(txPdcpPduBytesNrRlc),
        // DRB.PdcpPduNbrDl.Qos.UEID(txPdcpPduNrRlc)

        // the last two are not available on LTE
        for (auto ue : ueMap)
        {
            uint64_t imsi = ue.second->GetImsi();
            std::string ueImsiComplete = GetImsiString(imsi);

            auto uePms = uePmString.find(imsi)->second;

            std::string to_print = std::to_string(timestamp) + "," + ueImsiComplete + "," +
                                   std::to_string(cellAverageLatency) + "," + std::to_string(0) +
                                   "," + std::to_string(cellDlTxVolume) + "," + uePms + "\n";

            csv << to_print;
        }
        csv.close();
        return nullptr;
    }
    else
    {
        return indicationMessageHelper->CreateIndicationMessage();
    }
}

void
LteEnbNetDevice::SaveActiveUes(void)
{
    NS_LOG_FUNCTION(this);
    m_ueRrcMean.push_back(m_rrc->GetUeMap().size());

    // We register this every 10 ms
    Simulator::Schedule(MilliSeconds(10), &LteEnbNetDevice::SaveActiveUes, this);
}

long
LteEnbNetDevice::ComputeMeanUes(void)
{
    NS_LOG_FUNCTION(this);
    if (m_ueRrcMean.size() == 0)
    {
        return 0;
    }
    // result is an integer according to the standard
    long meanUes =
        std::round(std::accumulate(m_ueRrcMean.begin(), m_ueRrcMean.end(), 0) / m_ueRrcMean.size());

    m_ueRrcMean.clear();
    return meanUes;
}

Ptr<KpmIndicationMessage>
LteEnbNetDevice::BuildRicIndicationMessageCuCp(std::string plmId)
{
    Ptr<LteIndicationMessageHelper> indicationMessageHelper =
        Create<LteIndicationMessageHelper>(IndicationMessageHelper::IndicationMessageType::CuCp,
                                           m_forceE2FileLogging,
                                           m_reducedPmValues);

    auto ueMap = m_rrc->GetUeMap();
    auto ueMapSize = ueMap.size();
    long meanRrcUes = ComputeMeanUes();

    std::unordered_map<uint64_t, std::string> uePmString{};

    for (auto ue : ueMap)
    {
        uint64_t imsi = ue.second->GetImsi();
        std::string ueImsiComplete = GetImsiString(imsi);
        long numDrb = ue.second->GetDrbMap().size();

        if (!indicationMessageHelper->IsOffline())
        {
            // DRB.RelActNbr.5QI.UEID not modeled in the simulator
            indicationMessageHelper->AddCuCpUePmItem(ueImsiComplete, numDrb, 0);
        }

        uePmString.insert(std::make_pair(imsi, std::to_string(numDrb) + "," + std::to_string(0)));
    }

    if (!indicationMessageHelper->IsOffline())
    {
        indicationMessageHelper->FillCuCpValues(ueMapSize);
    }

    if (m_forceE2FileLogging)
    {
        std::ofstream csv{};
        csv.open(m_cuCpFileName.c_str(), std::ios_base::app);
        if (!csv.is_open())
        {
            NS_FATAL_ERROR("Can't open file " << m_cuCpFileName.c_str());
        }

        NS_LOG_DEBUG("m_cuCpFileName open " << m_cuCpFileName);

        // the string is timestamp, ueImsiComplete, numActiveUes, RRC.ConnMean,
        // DRB.EstabSucc.5QI.UEID (numDrb), DRB.RelActNbr.5QI.UEID (0)

        uint64_t timestamp = m_startTime + Simulator::Now().GetMilliSeconds();

        for (auto ue : ueMap)
        {
            uint64_t imsi = ue.second->GetImsi();
            std::string ueImsiComplete = GetImsiString(imsi);

            auto uePms = uePmString.find(imsi)->second;

            // SINR for the same cell
            ImsiCellIdPair_t cid{imsi, m_cellId};
            double sinrThisCell = 10 * std::log10(m_l3sinrMap[cid]);
            double convertedSinr = L3RrcMeasurements::ThreeGppMapSinr(sinrThisCell);

            std::string to_print =
                std::to_string(timestamp) + "," + ueImsiComplete + "," + std::to_string(ueMapSize) +
                "," + std::to_string(meanRrcUes)+ "," + uePms + "," + std::to_string(m_cellId) + "," +
                std::to_string(sinrThisCell) + "," + std::to_string(convertedSinr) + "\n";

            // NS_LOG_DEBUG(to_print);

            csv << to_print;
        }

        csv.close();

        return nullptr;
    }
    else
    {
        return indicationMessageHelper->CreateIndicationMessage();
    }
}

void
LteEnbNetDevice::BuildAndSendReportMessage(E2Termination::RicSubscriptionRequest_rval_s params)
{
    std::string plmId = "111";
    std::string gnbId = std::to_string(m_cellId);

    // TODO here we can get something from RRC and onward
    NS_LOG_DEBUG("LteEnbNetDevice " << std::to_string(m_cellId) << " BuildAndSendMessage at time "
                                    << Simulator::Now().GetSeconds());

    if (m_sendCuUp)
    {
        // Create CU-UP
        Ptr<KpmIndicationHeader> header = BuildRicIndicationHeader(plmId, gnbId, m_cellId);
        Ptr<KpmIndicationMessage> cuUpMsg = BuildRicIndicationMessageCuUp(plmId);

        // Send CU-UP only if offline logging is disabled
        if (!m_forceE2FileLogging && header != nullptr && cuUpMsg != nullptr)
        {
            NS_LOG_DEBUG("Send LTE CU-UP");
            E2AP_PDU* pdu_cuup_ue = new E2AP_PDU;
            encoding::generate_e2apv1_indication_request_parameterized(
                pdu_cuup_ue,
                params.requestorId,
                params.instanceId,
                params.ranFuncionId,
                params.actionId,
                1,                           // TODO sequence number
                (uint8_t*)header->m_buffer,  // buffer containing the encoded header
                header->m_size,              // size of the encoded header
                (uint8_t*)cuUpMsg->m_buffer, // buffer containing the encoded message
                cuUpMsg->m_size);            // size of the encoded message
            m_e2term->SendE2Message(pdu_cuup_ue);
            delete pdu_cuup_ue;
        }
    }

    if (m_sendCuCp)
    {
        // Create CU-CP
        Ptr<KpmIndicationHeader> header = BuildRicIndicationHeader(plmId, gnbId, m_cellId);
        Ptr<KpmIndicationMessage> cuCpMsg = BuildRicIndicationMessageCuCp(plmId);

        // Send CU-CP only if offline logging is disabled
        if (!m_forceE2FileLogging && header != nullptr && cuCpMsg != nullptr)
        {
            NS_LOG_DEBUG("Send LTE CU-CP");
            E2AP_PDU* pdu_cucp_ue = new E2AP_PDU;
            encoding::generate_e2apv1_indication_request_parameterized(
                pdu_cucp_ue,
                params.requestorId,
                params.instanceId,
                params.ranFuncionId,
                params.actionId,
                1,                           // TODO sequence number
                (uint8_t*)header->m_buffer,  // buffer containing the encoded header
                header->m_size,              // size of the encoded header
                (uint8_t*)cuCpMsg->m_buffer, // buffer containing the encoded message
                cuCpMsg->m_size);            // size of the encoded message
            m_e2term->SendE2Message(pdu_cucp_ue);
            delete pdu_cucp_ue;
        }
    }

    if (!m_forceE2FileLogging)
        Simulator::ScheduleWithContext(1,
                                       Seconds(m_e2Periodicity),
                                       &LteEnbNetDevice::BuildAndSendReportMessage,
                                       this,
                                       params);
    else
        Simulator::Schedule(Seconds(m_e2Periodicity),
                            &LteEnbNetDevice::BuildAndSendReportMessage,
                            this,
                            params);
}

void
LteEnbNetDevice::SetStartTime(uint64_t st)
{
    m_startTime = st;
}


uint64_t
LteEnbNetDevice::GetStartTime()
{
    return m_startTime;
}

double
LteEnbNetDevice::GetE2Periodicity ()
{
    return m_e2Periodicity;
}

} // namespace ns3
