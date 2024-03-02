<?xml version="1.0"?>
<PlacementProgram xmlns="http://api.assembleon.com/pp/v2">
	<General syntaxVersion="2.0" lastModified="2022-11-26T15:19:59Z" positionInLine="2" machineModel="AX-501" operationMode="LOCAL" cycleTime="29290">
		<Setting key="PPS_Product" value="Optimizer"/>
		<Setting key="PPS_Version" value="8.245.4"/>
		<Setting key="PPS_Date" value="2021-09-23"/>
		<Setting key="PPS_Time" value="08:53:14"/>
		<Setting key="PPS_Features" value="COMBINED: Enabled, TALON: Enabled, EXPENSIVE_ASSERTS: Disabled"/>
		<Setting key="enableNeighborhoodRestrictions" value="false"/>
		<Setting key="doNotDuplicatePreassignment" value="false"/>
		<Setting key="alignmentMatrixPerCircuit" value="false"/>
		<Setting key="maxNoFiducialsPerAlignmentMatrix" value="3"/>
		<Setting key="mixedProductionMode" value="false"/>
		<Setting key="boardAccuracyClass" value="HIGH"/>
		<Setting key="userDefinedAlignmentMatrices" value="false"/>
		<Setting key="lineBalanceStrategy" value="equallyBalanced"/>
		<Setting key="speedPack" value="SP2"/>
		<Setting key="spliceTapeLength" value="0.0"/>
		<Setting key="badmarkMeasurementMode" value="USED"/>
		<Setting key="circuitFiducialsRequirePanelFiducials" value="false"/>
		<Setting key="highAccuracy" value="false"/>
		<Setting key="onlineWorkAreaCalibration" value="true"/>
		<Setting key="preferTTF" value="false"/>
		<Setting key="supportedBoardLengthType" value="STANDARD"/>
		<Setting key="indexScheme_maxBoardsOnBeam" value="10"/>
		<Setting key="indexScheme_stopperUsage" value="STOPPER1_ONLY"/>
		<Setting key="indexScheme_supportPinsOnBeamExtensions" value="NOT_APPLICABLE"/>
		<Setting key="performPrePick" value="true"/>
		<Setting key="boardOrientation" value="STANDARD"/>
		<Setting key="maxRunTime" value="43200"/>
		<Setting key="maxRunTimeWithFeasible" value="43200"/>
		<Setting key="maxEvaluations" value="2147483647"/>
		<Setting key="targetCycleTime" value="0"/>
		<Setting key="stopAtFirstFeasible" value="false"/>
		<Setting key="divergenceRate" value="0.25"/>
		<Setting key="maxDivergence" value="10"/>
		<Setting key="maxNonImprDivergence" value="3"/>
		<Setting key="minAcceptCount" value="1"/>
		<Setting key="crossoverSeed" value="56789"/>
		<Setting key="initSeed" value="12345"/>
		<Setting key="mutationSeed" value="54321"/>
		<Setting key="selectionSeed" value="65535"/>
		<Setting key="maxZeroAccepts" value="10"/>
		<Setting key="populationSize" value="50"/>
		<Setting key="logging.logFileName" value="c:\logfile.xml"/>
		<Setting key="logging.createLogFile" value="false"/>
		<Setting key="useOptimizerMonitor" value="true"/>
		<Setting key="outputReadFiducialActionsOnly" value="false"/>
		<Setting key="useFeedersWithShiftableTopGuide" value="true"/>
		<Setting key="soapServer.generateNewSolutionMessages" value="true"/>
		<Setting key="enableFeasibilityChecker" value="true"/>
		<Setting key="reduceRotationOnAx" value="true"/>
		<Setting key="newSchemeFinderTrigger" value="0.80"/>
		<Setting key="maxDuplicationFactor" value="100.0"/>
		<Setting key="maxComponentsPerCircuitForMinimizingAlignmentMatrices" value="40"/>
		<Setting key="alternateFeedingStrategy" value="disallowAlternateOnSameFeeder"/>
	</General>
	<Board id="3440CB-SD9-M5-IT-123459-PARTSUB-ALL" length="269.737" width="220.917" thickness="1.250" cadOriginX="0.000" cadOriginY="0.000" handlingClass="HIGH" maxComponentHeightNonProducingSide="20.000">
		<Badmark modelId="r1574.8" x="-185.138" y="113.459" z="0.000" rz="0.00" refDes="B0" circuitNumber="1"/>
		<Badmark modelId="r1574.8" x="-48.998" y="113.459" z="0.000" rz="0.00" refDes="B1" circuitNumber="2"/>
		<Badmark modelId="r1574.8" x="-185.138" y="8.659" z="0.000" rz="0.00" refDes="B2" circuitNumber="3"/>
		<Badmark modelId="r1574.8" x="-48.998" y="8.659" z="0.000" rz="0.00" refDes="B3" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-256.838" y="166.959" z="0.000" rz="0.00" refDes="F0_0" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-237.998" y="162.599" z="0.000" rz="0.00" refDes="F0_1" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-151.838" y="201.559" z="0.000" rz="0.00" refDes="F0_2" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-195.188" y="118.259" z="0.000" rz="0.00" refDes="F0_3" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-261.738" y="127.459" z="0.000" rz="0.00" refDes="F0_4" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-167.638" y="113.459" z="0.000" rz="0.00" refDes="F0_5" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-212.238" y="193.859" z="0.000" rz="0.00" refDes="F0_6" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-120.698" y="166.959" z="0.000" rz="0.00" refDes="F1_0" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-101.858" y="162.599" z="0.000" rz="0.00" refDes="F1_1" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-15.698" y="201.559" z="0.000" rz="0.00" refDes="F1_2" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-59.048" y="118.259" z="0.000" rz="0.00" refDes="F1_3" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-125.598" y="127.459" z="0.000" rz="0.00" refDes="F1_4" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-31.498" y="113.459" z="0.000" rz="0.00" refDes="F1_5" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-76.098" y="193.859" z="0.000" rz="0.00" refDes="F1_6" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-256.838" y="62.159" z="0.000" rz="0.00" refDes="F2_0" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-237.998" y="57.799" z="0.000" rz="0.00" refDes="F2_1" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-151.838" y="96.759" z="0.000" rz="0.00" refDes="F2_2" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-195.188" y="13.459" z="0.000" rz="0.00" refDes="F2_3" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-261.738" y="22.659" z="0.000" rz="0.00" refDes="F2_4" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-167.638" y="8.659" z="0.000" rz="0.00" refDes="F2_5" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-212.238" y="89.059" z="0.000" rz="0.00" refDes="F2_6" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-120.698" y="62.159" z="0.000" rz="0.00" refDes="F3_0" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-101.858" y="57.799" z="0.000" rz="0.00" refDes="F3_1" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-15.698" y="96.759" z="0.000" rz="0.00" refDes="F3_2" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-59.048" y="13.459" z="0.000" rz="0.00" refDes="F3_3" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-125.598" y="22.659" z="0.000" rz="0.00" refDes="F3_4" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-31.498" y="8.659" z="0.000" rz="0.00" refDes="F3_5" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-76.098" y="89.059" z="0.000" rz="0.00" refDes="F3_6" circuitNumber="4"/>
		<AlignmentMatrix id="AM_1" type="CIRCUIT">
			<FiducialReference refId="0_0_3_F3_1_4" refDes="F3_1" circuitNumber="4"/>
			<FiducialReference refId="0_0_3_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="0_0_3_F3_5_4" refDes="F3_5" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_10" type="CIRCUIT">
			<FiducialReference refId="2_1_3_F3_4_4" refDes="F3_4" circuitNumber="4"/>
			<FiducialReference refId="2_1_3_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="2_1_3_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_11" type="CIRCUIT">
			<FiducialReference refId="1_1_3_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="1_1_3_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="1_1_3_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_12" type="CIRCUIT">
			<FiducialReference refId="3_2_0_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="3_2_0_F3_5_4" refDes="F3_5" circuitNumber="4"/>
			<FiducialReference refId="3_2_0_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_13" type="CIRCUIT">
			<FiducialReference refId="2_2_0_F0_3_1" refDes="F0_3" circuitNumber="1"/>
			<FiducialReference refId="2_2_0_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="2_2_0_F0_2_1" refDes="F0_2" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_14" type="CIRCUIT">
			<FiducialReference refId="1_2_0_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="1_2_0_F0_0_1" refDes="F0_0" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_15" type="CIRCUIT">
			<FiducialReference refId="4_2_1_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="4_2_1_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="4_2_1_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_16" type="CIRCUIT">
			<FiducialReference refId="3_2_1_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="3_2_1_F3_4_4" refDes="F3_4" circuitNumber="4"/>
			<FiducialReference refId="3_2_1_F1_6_2" refDes="F1_6" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_17" type="CIRCUIT">
			<FiducialReference refId="2_2_1_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="2_2_1_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="2_2_1_F2_6_3" refDes="F2_6" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_18" type="CIRCUIT">
			<FiducialReference refId="4_2_2_F3_1_4" refDes="F3_1" circuitNumber="4"/>
			<FiducialReference refId="4_2_2_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="4_2_2_F3_5_4" refDes="F3_5" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_19" type="CIRCUIT">
			<FiducialReference refId="3_2_2_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="3_2_2_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="3_2_2_F0_2_1" refDes="F0_2" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_2" type="CIRCUIT">
			<FiducialReference refId="1_1_0_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="1_1_0_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="1_1_0_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_20" type="CIRCUIT">
			<FiducialReference refId="5_2_3_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="5_2_3_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="5_2_3_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_21" type="CIRCUIT">
			<FiducialReference refId="4_2_3_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="4_2_3_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="4_2_3_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_22" type="CIRCUIT">
			<FiducialReference refId="3_2_3_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="3_2_3_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="3_2_3_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_23" type="CIRCUIT">
			<FiducialReference refId="5_3_0_F3_1_4" refDes="F3_1" circuitNumber="4"/>
			<FiducialReference refId="5_3_0_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="5_3_0_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_24" type="CIRCUIT">
			<FiducialReference refId="4_3_0_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="4_3_0_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="4_3_0_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_25" type="CIRCUIT">
			<FiducialReference refId="6_3_1_F1_3_2" refDes="F1_3" circuitNumber="2"/>
			<FiducialReference refId="6_3_1_F3_5_4" refDes="F3_5" circuitNumber="4"/>
			<FiducialReference refId="6_3_1_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_26" type="CIRCUIT">
			<FiducialReference refId="5_3_1_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="5_3_1_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="5_3_1_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_27" type="CIRCUIT">
			<FiducialReference refId="4_3_1_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="4_3_1_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="4_3_1_F2_1_3" refDes="F2_1" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_29" type="CIRCUIT">
			<FiducialReference refId="6_3_2_F3_4_4" refDes="F3_4" circuitNumber="4"/>
			<FiducialReference refId="6_3_2_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="6_3_2_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_3" type="CIRCUIT">
			<FiducialReference refId="0_1_0_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="0_1_0_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="0_1_0_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_30" type="CIRCUIT">
			<FiducialReference refId="5_3_2_F2_0_3" refDes="F2_0" circuitNumber="3"/>
			<FiducialReference refId="5_3_2_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="5_3_2_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_31" type="CIRCUIT">
			<FiducialReference refId="7_3_3_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="7_3_3_F3_5_4" refDes="F3_5" circuitNumber="4"/>
			<FiducialReference refId="7_3_3_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_32" type="CIRCUIT">
			<FiducialReference refId="6_3_3_F0_3_1" refDes="F0_3" circuitNumber="1"/>
			<FiducialReference refId="6_3_3_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="6_3_3_F0_2_1" refDes="F0_2" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_33" type="CIRCUIT">
			<FiducialReference refId="5_3_3_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="5_3_3_F0_0_1" refDes="F0_0" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_34" type="CIRCUIT">
			<FiducialReference refId="8_4_0_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="8_4_0_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="8_4_0_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_35" type="CIRCUIT">
			<FiducialReference refId="7_4_0_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="7_4_0_F3_4_4" refDes="F3_4" circuitNumber="4"/>
			<FiducialReference refId="7_4_0_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_36" type="CIRCUIT">
			<FiducialReference refId="6_4_0_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="6_4_0_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="6_4_0_F2_6_3" refDes="F2_6" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_37" type="CIRCUIT">
			<FiducialReference refId="8_4_1_F3_1_4" refDes="F3_1" circuitNumber="4"/>
			<FiducialReference refId="8_4_1_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="8_4_1_F3_5_4" refDes="F3_5" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_38" type="CIRCUIT">
			<FiducialReference refId="7_4_1_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="7_4_1_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="7_4_1_F0_2_1" refDes="F0_2" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_39" type="CIRCUIT">
			<FiducialReference refId="9_4_2_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="9_4_2_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="9_4_2_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_4" type="CIRCUIT">
			<FiducialReference refId="1_1_1_F3_1_4" refDes="F3_1" circuitNumber="4"/>
			<FiducialReference refId="1_1_1_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="1_1_1_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_40" type="CIRCUIT">
			<FiducialReference refId="8_4_2_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="8_4_2_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="8_4_2_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_41" type="CIRCUIT">
			<FiducialReference refId="7_4_2_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="7_4_2_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="7_4_2_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_42" type="CIRCUIT">
			<FiducialReference refId="9_4_3_F3_1_4" refDes="F3_1" circuitNumber="4"/>
			<FiducialReference refId="9_4_3_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="9_4_3_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_43" type="CIRCUIT">
			<FiducialReference refId="8_4_3_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="8_4_3_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="8_4_3_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_44" type="CIRCUIT">
			<FiducialReference refId="9_5_0_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="9_5_0_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="9_5_0_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_46" type="CIRCUIT">
			<FiducialReference refId="8_5_0_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="8_5_0_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="8_5_0_F2_1_3" refDes="F2_1" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_47" type="CIRCUIT">
			<FiducialReference refId="9_5_1_F2_0_3" refDes="F2_0" circuitNumber="3"/>
			<FiducialReference refId="9_5_1_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="9_5_1_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_48" type="CIRCUIT">
			<FiducialReference refId="10_5_1_F3_4_4" refDes="F3_4" circuitNumber="4"/>
			<FiducialReference refId="10_5_1_F1_0_2" refDes="F1_0" circuitNumber="2"/>
			<FiducialReference refId="10_5_1_F3_6_4" refDes="F3_6" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_5" type="CIRCUIT">
			<FiducialReference refId="0_1_1_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="0_1_1_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="0_1_1_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_50" type="CIRCUIT">
			<FiducialReference refId="9_5_2_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="9_5_2_F0_0_1" refDes="F0_0" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_51" type="CIRCUIT">
			<FiducialReference refId="10_5_2_F0_3_1" refDes="F0_3" circuitNumber="1"/>
			<FiducialReference refId="10_5_2_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="10_5_2_F0_2_1" refDes="F0_2" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_52" type="CIRCUIT">
			<FiducialReference refId="11_5_2_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="11_5_2_F3_5_4" refDes="F3_5" circuitNumber="4"/>
			<FiducialReference refId="11_5_2_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_53" type="CIRCUIT">
			<FiducialReference refId="10_5_3_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="10_5_3_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="10_5_3_F2_6_3" refDes="F2_6" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_54" type="CIRCUIT">
			<FiducialReference refId="11_5_3_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="11_5_3_F3_4_4" refDes="F3_4" circuitNumber="4"/>
			<FiducialReference refId="11_5_3_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_56" type="CIRCUIT">
			<FiducialReference refId="11_6_0_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="11_6_0_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="11_6_0_F0_2_1" refDes="F0_2" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_57" type="CIRCUIT">
			<FiducialReference refId="12_6_0_F3_1_4" refDes="F3_1" circuitNumber="4"/>
			<FiducialReference refId="12_6_0_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="12_6_0_F3_5_4" refDes="F3_5" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_58" type="CIRCUIT">
			<FiducialReference refId="11_6_1_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="11_6_1_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="11_6_1_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_59" type="CIRCUIT">
			<FiducialReference refId="12_6_1_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="12_6_1_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="12_6_1_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_6" type="CIRCUIT">
			<FiducialReference refId="2_1_2_F1_3_2" refDes="F1_3" circuitNumber="2"/>
			<FiducialReference refId="2_1_2_F3_5_4" refDes="F3_5" circuitNumber="4"/>
			<FiducialReference refId="2_1_2_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_60" type="CIRCUIT">
			<FiducialReference refId="13_6_1_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="13_6_1_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="13_6_1_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_61" type="CIRCUIT">
			<FiducialReference refId="12_6_2_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="12_6_2_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="12_6_2_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_62" type="CIRCUIT">
			<FiducialReference refId="13_6_2_F3_0_4" refDes="F3_0" circuitNumber="4"/>
			<FiducialReference refId="13_6_2_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="13_6_2_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_63" type="CIRCUIT">
			<FiducialReference refId="12_6_3_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="12_6_3_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="12_6_3_F2_1_3" refDes="F2_1" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_64" type="CIRCUIT">
			<FiducialReference refId="13_6_3_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="13_6_3_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="13_6_3_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_65" type="CIRCUIT">
			<FiducialReference refId="14_6_3_F1_3_2" refDes="F1_3" circuitNumber="2"/>
			<FiducialReference refId="14_6_3_F3_5_4" refDes="F3_5" circuitNumber="4"/>
			<FiducialReference refId="14_6_3_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_66" type="CIRCUIT">
			<FiducialReference refId="13_7_0_F2_0_3" refDes="F2_0" circuitNumber="3"/>
			<FiducialReference refId="13_7_0_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="13_7_0_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_67" type="CIRCUIT">
			<FiducialReference refId="14_7_0_F3_4_4" refDes="F3_4" circuitNumber="4"/>
			<FiducialReference refId="14_7_0_F1_0_2" refDes="F1_0" circuitNumber="2"/>
			<FiducialReference refId="14_7_0_F3_6_4" refDes="F3_6" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_68" type="CIRCUIT">
			<FiducialReference refId="15_7_0_F1_2_2" refDes="F1_2" circuitNumber="2"/>
			<FiducialReference refId="15_7_0_F3_2_4" refDes="F3_2" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_7" type="CIRCUIT">
			<FiducialReference refId="1_1_2_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="1_1_2_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="1_1_2_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_70" type="CIRCUIT">
			<FiducialReference refId="14_7_1_F0_3_1" refDes="F0_3" circuitNumber="1"/>
			<FiducialReference refId="14_7_1_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="14_7_1_F0_2_1" refDes="F0_2" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_71" type="CIRCUIT">
			<FiducialReference refId="15_7_1_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="15_7_1_F3_5_4" refDes="F3_5" circuitNumber="4"/>
			<FiducialReference refId="15_7_1_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_72" type="CIRCUIT">
			<FiducialReference refId="14_7_2_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="14_7_2_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="14_7_2_F2_6_3" refDes="F2_6" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_73" type="CIRCUIT">
			<FiducialReference refId="15_7_2_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="15_7_2_F3_4_4" refDes="F3_4" circuitNumber="4"/>
			<FiducialReference refId="15_7_2_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_75" type="CIRCUIT">
			<FiducialReference refId="15_7_3_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="15_7_3_F2_3_3" refDes="F2_3" circuitNumber="3"/>
			<FiducialReference refId="15_7_3_F0_2_1" refDes="F0_2" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_76" type="CIRCUIT">
			<FiducialReference refId="16_7_3_F3_1_4" refDes="F3_1" circuitNumber="4"/>
			<FiducialReference refId="16_7_3_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="16_7_3_F3_5_4" refDes="F3_5" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_77" type="CIRCUIT">
			<FiducialReference refId="15_8_0_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="15_8_0_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="15_8_0_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_78" type="CIRCUIT">
			<FiducialReference refId="16_8_0_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="16_8_0_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="16_8_0_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_79" type="CIRCUIT">
			<FiducialReference refId="17_8_0_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="17_8_0_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="17_8_0_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_8" type="CIRCUIT">
			<FiducialReference refId="0_1_2_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="0_1_2_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="0_1_2_F2_1_3" refDes="F2_1" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_80" type="CIRCUIT">
			<FiducialReference refId="16_8_1_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="16_8_1_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="16_8_1_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_81" type="CIRCUIT">
			<FiducialReference refId="17_8_1_F3_0_4" refDes="F3_0" circuitNumber="4"/>
			<FiducialReference refId="17_8_1_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="17_8_1_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_82" type="CIRCUIT">
			<FiducialReference refId="16_8_2_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="16_8_2_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="16_8_2_F2_1_3" refDes="F2_1" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_83" type="CIRCUIT">
			<FiducialReference refId="17_8_2_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="17_8_2_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="17_8_2_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_85" type="CIRCUIT">
			<FiducialReference refId="17_8_3_F2_0_3" refDes="F2_0" circuitNumber="3"/>
			<FiducialReference refId="17_8_3_F0_6_1" refDes="F0_6" circuitNumber="1"/>
			<FiducialReference refId="17_8_3_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_86" type="CIRCUIT">
			<FiducialReference refId="18_8_3_F1_4_2" refDes="F1_4" circuitNumber="2"/>
			<FiducialReference refId="18_8_3_F3_5_4" refDes="F3_5" circuitNumber="4"/>
			<FiducialReference refId="18_8_3_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_88" type="CIRCUIT">
			<FiducialReference refId="18_9_0_F0_3_1" refDes="F0_3" circuitNumber="1"/>
			<FiducialReference refId="18_9_0_F3_5_4" refDes="F3_5" circuitNumber="4"/>
			<FiducialReference refId="18_9_0_F1_2_2" refDes="F1_2" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_89" type="CIRCUIT">
			<FiducialReference refId="18_9_1_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="18_9_1_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="18_9_1_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_9" type="CIRCUIT">
			<FiducialReference refId="3_1_3_F1_2_2" refDes="F1_2" circuitNumber="2"/>
			<FiducialReference refId="3_1_3_F3_2_4" refDes="F3_2" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_90" type="CIRCUIT">
			<FiducialReference refId="18_9_2_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="18_9_2_F2_5_3" refDes="F2_5" circuitNumber="3"/>
			<FiducialReference refId="18_9_2_F0_2_1" refDes="F0_2" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_91" type="CIRCUIT">
			<FiducialReference refId="18_9_3_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="18_9_3_F2_1_3" refDes="F2_1" circuitNumber="3"/>
			<FiducialReference refId="18_9_3_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<Component partNumber="510-500866" refDes="R240" circuitNumber="1" x="-267.988" y="153.929" z="0.000" rz="90.00"/>
		<Component partNumber="510-500866" refDes="R240" circuitNumber="2" x="-131.848" y="153.929" z="0.000" rz="90.00"/>
		<Component partNumber="510-500866" refDes="R240" circuitNumber="3" x="-267.988" y="49.129" z="0.000" rz="90.00"/>
		<Component partNumber="510-500866" refDes="R240" circuitNumber="4" x="-131.848" y="49.129" z="0.000" rz="90.00"/>
		<Component partNumber="510-500879" refDes="R267" circuitNumber="1" x="-249.607" y="187.573" z="0.000" rz="0.00"/>
		<Component partNumber="510-500879" refDes="R267" circuitNumber="2" x="-113.467" y="187.573" z="0.000" rz="0.00"/>
		<Component partNumber="510-500879" refDes="R267" circuitNumber="3" x="-249.607" y="82.773" z="0.000" rz="0.00"/>
		<Component partNumber="510-500879" refDes="R267" circuitNumber="4" x="-113.467" y="82.773" z="0.000" rz="0.00"/>
		<Component partNumber="510-500879" refDes="R425" circuitNumber="1" x="-217.538" y="127.059" z="0.000" rz="180.00"/>
		<Component partNumber="510-500879" refDes="R425" circuitNumber="2" x="-81.398" y="127.059" z="0.000" rz="180.00"/>
		<Component partNumber="510-500879" refDes="R425" circuitNumber="3" x="-217.538" y="22.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-500879" refDes="R425" circuitNumber="4" x="-81.398" y="22.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-500881" refDes="R214" circuitNumber="1" x="-246.028" y="128.645" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R214" circuitNumber="2" x="-109.888" y="128.645" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R214" circuitNumber="3" x="-246.028" y="23.845" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R214" circuitNumber="4" x="-109.888" y="23.845" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R225" circuitNumber="1" x="-246.028" y="140.202" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R225" circuitNumber="2" x="-109.888" y="140.202" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R225" circuitNumber="3" x="-246.028" y="35.402" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R225" circuitNumber="4" x="-109.888" y="35.402" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R234" circuitNumber="1" x="-246.028" y="151.632" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R234" circuitNumber="2" x="-109.888" y="151.632" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R234" circuitNumber="3" x="-246.028" y="46.832" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R234" circuitNumber="4" x="-109.888" y="46.832" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R247" circuitNumber="1" x="-241.456" y="163.189" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R247" circuitNumber="2" x="-105.316" y="163.189" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R247" circuitNumber="3" x="-241.456" y="58.389" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R247" circuitNumber="4" x="-105.316" y="58.389" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R250" circuitNumber="1" x="-241.456" y="174.746" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R250" circuitNumber="2" x="-105.316" y="174.746" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R250" circuitNumber="3" x="-241.456" y="69.946" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R250" circuitNumber="4" x="-105.316" y="69.946" z="0.000" rz="90.00"/>
		<Component partNumber="510-500951" refDes="R269" circuitNumber="1" x="-248.158" y="189.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-500951" refDes="R269" circuitNumber="2" x="-112.018" y="189.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-500951" refDes="R269" circuitNumber="3" x="-248.158" y="84.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-500951" refDes="R269" circuitNumber="4" x="-112.018" y="84.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-500951" refDes="R270" circuitNumber="1" x="-249.858" y="190.059" z="0.000" rz="180.00"/>
		<Component partNumber="510-500951" refDes="R270" circuitNumber="2" x="-113.718" y="190.059" z="0.000" rz="180.00"/>
		<Component partNumber="510-500951" refDes="R270" circuitNumber="3" x="-249.858" y="85.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-500951" refDes="R270" circuitNumber="4" x="-113.718" y="85.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-500955" refDes="R235" circuitNumber="1" x="-267.488" y="149.389" z="0.000" rz="270.00"/>
		<Component partNumber="510-500955" refDes="R235" circuitNumber="2" x="-131.348" y="149.389" z="0.000" rz="270.00"/>
		<Component partNumber="510-500955" refDes="R235" circuitNumber="3" x="-267.488" y="44.589" z="0.000" rz="270.00"/>
		<Component partNumber="510-500955" refDes="R235" circuitNumber="4" x="-131.348" y="44.589" z="0.000" rz="270.00"/>
		<Component partNumber="510-500971" refDes="R77" circuitNumber="1" x="-267.870" y="138.528" z="0.000" rz="270.00"/>
		<Component partNumber="510-500971" refDes="R77" circuitNumber="2" x="-131.730" y="138.528" z="0.000" rz="270.00"/>
		<Component partNumber="510-500971" refDes="R77" circuitNumber="3" x="-267.870" y="33.728" z="0.000" rz="270.00"/>
		<Component partNumber="510-500971" refDes="R77" circuitNumber="4" x="-131.730" y="33.728" z="0.000" rz="270.00"/>
		<Component partNumber="510-500980" refDes="R264" circuitNumber="1" x="-218.850" y="187.319" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R264" circuitNumber="2" x="-82.710" y="187.319" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R264" circuitNumber="3" x="-218.850" y="82.519" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R264" circuitNumber="4" x="-82.710" y="82.519" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R275" circuitNumber="1" x="-219.788" y="190.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R275" circuitNumber="2" x="-83.648" y="190.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R275" circuitNumber="3" x="-219.788" y="85.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R275" circuitNumber="4" x="-83.648" y="85.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R285" circuitNumber="1" x="-220.238" y="194.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R285" circuitNumber="2" x="-84.098" y="194.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R285" circuitNumber="3" x="-220.238" y="89.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R285" circuitNumber="4" x="-84.098" y="89.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R305" circuitNumber="1" x="-214.611" y="208.740" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R305" circuitNumber="2" x="-78.471" y="208.740" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R305" circuitNumber="3" x="-214.611" y="103.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R305" circuitNumber="4" x="-78.471" y="103.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-500998" refDes="R215" circuitNumber="1" x="-242.670" y="129.093" z="0.000" rz="0.00"/>
		<Component partNumber="510-500998" refDes="R215" circuitNumber="2" x="-106.530" y="129.093" z="0.000" rz="0.00"/>
		<Component partNumber="510-500998" refDes="R215" circuitNumber="3" x="-242.670" y="24.293" z="0.000" rz="0.00"/>
		<Component partNumber="510-500998" refDes="R215" circuitNumber="4" x="-106.530" y="24.293" z="0.000" rz="0.00"/>
		<Component partNumber="510-500998" refDes="R306" circuitNumber="1" x="-215.373" y="208.740" z="0.000" rz="90.00"/>
		<Component partNumber="510-500998" refDes="R306" circuitNumber="2" x="-79.233" y="208.740" z="0.000" rz="90.00"/>
		<Component partNumber="510-500998" refDes="R306" circuitNumber="3" x="-215.373" y="103.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-500998" refDes="R306" circuitNumber="4" x="-79.233" y="103.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-500998" refDes="R307" circuitNumber="1" x="-216.135" y="208.740" z="0.000" rz="90.00"/>
		<Component partNumber="510-500998" refDes="R307" circuitNumber="2" x="-79.995" y="208.740" z="0.000" rz="90.00"/>
		<Component partNumber="510-500998" refDes="R307" circuitNumber="3" x="-216.135" y="103.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-500998" refDes="R307" circuitNumber="4" x="-79.995" y="103.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R271" circuitNumber="1" x="-230.153" y="189.732" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R271" circuitNumber="2" x="-94.013" y="189.732" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R271" circuitNumber="3" x="-230.153" y="84.932" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R271" circuitNumber="4" x="-94.013" y="84.932" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R272" circuitNumber="1" x="-241.456" y="189.732" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R272" circuitNumber="2" x="-105.316" y="189.732" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R272" circuitNumber="3" x="-241.456" y="84.932" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R272" circuitNumber="4" x="-105.316" y="84.932" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R276" circuitNumber="1" x="-241.741" y="192.272" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R276" circuitNumber="2" x="-105.601" y="192.272" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R276" circuitNumber="3" x="-241.741" y="87.472" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R276" circuitNumber="4" x="-105.601" y="87.472" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R298" circuitNumber="1" x="-224.692" y="208.274" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R298" circuitNumber="2" x="-88.552" y="208.274" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R298" circuitNumber="3" x="-224.692" y="103.474" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R298" circuitNumber="4" x="-88.552" y="103.474" z="0.000" rz="270.00"/>
		<Component partNumber="510-501040" refDes="R290" circuitNumber="1" x="-223.388" y="200.729" z="0.000" rz="90.00"/>
		<Component partNumber="510-501040" refDes="R290" circuitNumber="2" x="-87.248" y="200.729" z="0.000" rz="90.00"/>
		<Component partNumber="510-501040" refDes="R290" circuitNumber="3" x="-223.388" y="95.929" z="0.000" rz="90.00"/>
		<Component partNumber="510-501040" refDes="R290" circuitNumber="4" x="-87.248" y="95.929" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R150" circuitNumber="1" x="-241.456" y="173.603" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R150" circuitNumber="2" x="-105.316" y="173.603" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R150" circuitNumber="3" x="-241.456" y="68.803" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R150" circuitNumber="4" x="-105.316" y="68.803" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R154" circuitNumber="1" x="-226.089" y="176.778" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R154" circuitNumber="2" x="-89.949" y="176.778" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R154" circuitNumber="3" x="-226.089" y="71.978" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R154" circuitNumber="4" x="-89.949" y="71.978" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R211" circuitNumber="1" x="-162.565" y="117.088" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R211" circuitNumber="2" x="-26.425" y="117.088" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R211" circuitNumber="3" x="-162.565" y="12.288" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R211" circuitNumber="4" x="-26.425" y="12.288" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R212" circuitNumber="1" x="-181.615" y="117.088" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R212" circuitNumber="2" x="-45.475" y="117.088" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R212" circuitNumber="3" x="-181.615" y="12.288" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R212" circuitNumber="4" x="-45.475" y="12.288" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R218" circuitNumber="1" x="-162.565" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R218" circuitNumber="2" x="-26.425" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R218" circuitNumber="3" x="-162.565" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R218" circuitNumber="4" x="-26.425" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R219" circuitNumber="1" x="-164.089" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R219" circuitNumber="2" x="-27.949" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R219" circuitNumber="3" x="-164.089" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R219" circuitNumber="4" x="-27.949" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R220" circuitNumber="1" x="-181.615" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R220" circuitNumber="2" x="-45.475" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R220" circuitNumber="3" x="-181.615" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R220" circuitNumber="4" x="-45.475" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R221" circuitNumber="1" x="-183.139" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R221" circuitNumber="2" x="-46.999" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R221" circuitNumber="3" x="-183.139" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R221" circuitNumber="4" x="-46.999" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R226" circuitNumber="1" x="-153.040" y="140.518" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R226" circuitNumber="2" x="-16.900" y="140.518" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R226" circuitNumber="3" x="-153.040" y="35.718" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R226" circuitNumber="4" x="-16.900" y="35.718" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R228" circuitNumber="1" x="-138.054" y="141.492" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R228" circuitNumber="2" x="-1.914" y="141.492" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R228" circuitNumber="3" x="-138.054" y="36.692" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R228" circuitNumber="4" x="-1.914" y="36.692" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R229" circuitNumber="1" x="-162.565" y="147.568" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R229" circuitNumber="2" x="-26.425" y="147.568" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R229" circuitNumber="3" x="-162.565" y="42.768" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R229" circuitNumber="4" x="-26.425" y="42.768" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R230" circuitNumber="1" x="-164.089" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R230" circuitNumber="2" x="-27.949" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R230" circuitNumber="3" x="-164.089" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R230" circuitNumber="4" x="-27.949" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R231" circuitNumber="1" x="-181.615" y="147.568" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R231" circuitNumber="2" x="-45.475" y="147.568" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R231" circuitNumber="3" x="-181.615" y="42.768" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R231" circuitNumber="4" x="-45.475" y="42.768" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R232" circuitNumber="1" x="-183.139" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R232" circuitNumber="2" x="-46.999" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R232" circuitNumber="3" x="-183.139" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R232" circuitNumber="4" x="-46.999" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R241" circuitNumber="1" x="-153.040" y="160.268" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R241" circuitNumber="2" x="-16.900" y="160.268" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R241" circuitNumber="3" x="-153.040" y="55.468" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R241" circuitNumber="4" x="-16.900" y="55.468" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R242" circuitNumber="1" x="-138.054" y="161.792" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R242" circuitNumber="2" x="-1.914" y="161.792" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R242" circuitNumber="3" x="-138.054" y="56.992" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R242" circuitNumber="4" x="-1.914" y="56.992" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R243" circuitNumber="1" x="-162.565" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R243" circuitNumber="2" x="-26.425" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R243" circuitNumber="3" x="-162.565" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R243" circuitNumber="4" x="-26.425" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R244" circuitNumber="1" x="-164.089" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R244" circuitNumber="2" x="-27.949" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R244" circuitNumber="3" x="-164.089" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R244" circuitNumber="4" x="-27.949" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R245" circuitNumber="1" x="-181.615" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R245" circuitNumber="2" x="-45.475" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R245" circuitNumber="3" x="-181.615" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R245" circuitNumber="4" x="-45.475" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R246" circuitNumber="1" x="-183.139" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R246" circuitNumber="2" x="-46.999" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R246" circuitNumber="3" x="-183.139" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R246" circuitNumber="4" x="-46.999" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R251" circuitNumber="1" x="-226.089" y="175.254" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R251" circuitNumber="2" x="-89.949" y="175.254" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R251" circuitNumber="3" x="-226.089" y="70.454" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R251" circuitNumber="4" x="-89.949" y="70.454" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R253" circuitNumber="1" x="-210.595" y="176.778" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R253" circuitNumber="2" x="-74.455" y="176.778" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R253" circuitNumber="3" x="-210.595" y="71.978" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R253" circuitNumber="4" x="-74.455" y="71.978" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R255" circuitNumber="1" x="-162.565" y="178.556" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R255" circuitNumber="2" x="-26.425" y="178.556" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R255" circuitNumber="3" x="-162.565" y="73.756" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R255" circuitNumber="4" x="-26.425" y="73.756" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R256" circuitNumber="1" x="-164.089" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R256" circuitNumber="2" x="-27.949" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R256" circuitNumber="3" x="-164.089" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R256" circuitNumber="4" x="-27.949" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R257" circuitNumber="1" x="-181.615" y="178.556" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R257" circuitNumber="2" x="-45.475" y="178.556" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R257" circuitNumber="3" x="-181.615" y="73.756" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R257" circuitNumber="4" x="-45.475" y="73.756" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R258" circuitNumber="1" x="-183.139" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R258" circuitNumber="2" x="-46.999" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R258" circuitNumber="3" x="-183.139" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R258" circuitNumber="4" x="-46.999" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R280" circuitNumber="1" x="-162.565" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R280" circuitNumber="2" x="-26.425" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R280" circuitNumber="3" x="-162.565" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R280" circuitNumber="4" x="-26.425" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R281" circuitNumber="1" x="-164.089" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R281" circuitNumber="2" x="-27.949" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R281" circuitNumber="3" x="-164.089" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R281" circuitNumber="4" x="-27.949" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R282" circuitNumber="1" x="-181.615" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R282" circuitNumber="2" x="-45.475" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R282" circuitNumber="3" x="-181.615" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R282" circuitNumber="4" x="-45.475" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R283" circuitNumber="1" x="-183.139" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R283" circuitNumber="2" x="-46.999" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R283" circuitNumber="3" x="-183.139" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R283" circuitNumber="4" x="-46.999" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R301" circuitNumber="1" x="-164.089" y="208.933" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R301" circuitNumber="2" x="-27.949" y="208.933" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R301" circuitNumber="3" x="-164.089" y="104.133" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R301" circuitNumber="4" x="-27.949" y="104.133" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R302" circuitNumber="1" x="-183.139" y="208.933" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R302" circuitNumber="2" x="-46.999" y="208.933" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R302" circuitNumber="3" x="-183.139" y="104.133" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R302" circuitNumber="4" x="-46.999" y="104.133" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R216" circuitNumber="1" x="-261.141" y="129.835" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R216" circuitNumber="2" x="-125.001" y="129.835" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R216" circuitNumber="3" x="-261.141" y="25.035" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R216" circuitNumber="4" x="-125.001" y="25.035" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R222" circuitNumber="1" x="-268.253" y="132.883" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R222" circuitNumber="2" x="-132.113" y="132.883" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R222" circuitNumber="3" x="-268.253" y="28.083" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R222" circuitNumber="4" x="-132.113" y="28.083" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R223" circuitNumber="1" x="-259.744" y="133.471" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R223" circuitNumber="2" x="-123.604" y="133.471" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R223" circuitNumber="3" x="-259.744" y="28.671" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R223" circuitNumber="4" x="-123.604" y="28.671" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R273" circuitNumber="1" x="-216.588" y="190.359" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R273" circuitNumber="2" x="-80.448" y="190.359" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R273" circuitNumber="3" x="-216.588" y="85.559" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R273" circuitNumber="4" x="-80.448" y="85.559" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R274" circuitNumber="1" x="-217.388" y="190.359" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R274" circuitNumber="2" x="-81.248" y="190.359" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R274" circuitNumber="3" x="-217.388" y="85.559" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R274" circuitNumber="4" x="-81.248" y="85.559" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R278" circuitNumber="1" x="-217.138" y="192.609" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R278" circuitNumber="2" x="-80.998" y="192.609" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R278" circuitNumber="3" x="-217.138" y="87.809" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R278" circuitNumber="4" x="-80.998" y="87.809" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R279" circuitNumber="1" x="-218.938" y="192.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R279" circuitNumber="2" x="-82.798" y="192.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R279" circuitNumber="3" x="-218.938" y="87.559" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R279" circuitNumber="4" x="-82.798" y="87.559" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R291" circuitNumber="1" x="-229.888" y="201.109" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R291" circuitNumber="2" x="-93.748" y="201.109" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R291" circuitNumber="3" x="-229.888" y="96.309" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R291" circuitNumber="4" x="-93.748" y="96.309" z="0.000" rz="0.00"/>
		<Component partNumber="510-501263" refDes="R295" circuitNumber="1" x="-233.328" y="207.512" z="0.000" rz="90.00"/>
		<Component partNumber="510-501263" refDes="R295" circuitNumber="2" x="-97.188" y="207.512" z="0.000" rz="90.00"/>
		<Component partNumber="510-501263" refDes="R295" circuitNumber="3" x="-233.328" y="102.712" z="0.000" rz="90.00"/>
		<Component partNumber="510-501263" refDes="R295" circuitNumber="4" x="-97.188" y="102.712" z="0.000" rz="90.00"/>
		<Component partNumber="510-501547" refDes="R73" circuitNumber="1" x="-266.638" y="137.689" z="0.000" rz="270.00"/>
		<Component partNumber="510-501547" refDes="R73" circuitNumber="2" x="-130.498" y="137.689" z="0.000" rz="270.00"/>
		<Component partNumber="510-501547" refDes="R73" circuitNumber="3" x="-266.638" y="32.889" z="0.000" rz="270.00"/>
		<Component partNumber="510-501547" refDes="R73" circuitNumber="4" x="-130.498" y="32.889" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R184" circuitNumber="1" x="-225.458" y="188.285" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R184" circuitNumber="2" x="-89.318" y="188.285" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R184" circuitNumber="3" x="-225.458" y="83.485" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R184" circuitNumber="4" x="-89.318" y="83.485" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R213" circuitNumber="1" x="-261.438" y="123.059" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R213" circuitNumber="2" x="-125.298" y="123.059" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R213" circuitNumber="3" x="-261.438" y="18.259" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R213" circuitNumber="4" x="-125.298" y="18.259" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R239" circuitNumber="1" x="-268.038" y="152.609" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R239" circuitNumber="2" x="-131.898" y="152.609" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R239" circuitNumber="3" x="-268.038" y="47.809" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R239" circuitNumber="4" x="-131.898" y="47.809" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R262" circuitNumber="1" x="-252.378" y="186.557" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R262" circuitNumber="2" x="-116.238" y="186.557" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R262" circuitNumber="3" x="-252.378" y="81.757" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R262" circuitNumber="4" x="-116.238" y="81.757" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R263" circuitNumber="1" x="-253.140" y="186.557" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R263" circuitNumber="2" x="-117.000" y="186.557" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R263" circuitNumber="3" x="-253.140" y="81.757" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R263" circuitNumber="4" x="-117.000" y="81.757" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R284" circuitNumber="1" x="-215.588" y="195.059" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R284" circuitNumber="2" x="-79.448" y="195.059" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R284" circuitNumber="3" x="-215.588" y="90.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R284" circuitNumber="4" x="-79.448" y="90.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R303" circuitNumber="1" x="-213.337" y="207.520" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R303" circuitNumber="2" x="-77.197" y="207.520" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R303" circuitNumber="3" x="-213.337" y="102.720" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R303" circuitNumber="4" x="-77.197" y="102.720" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R261" circuitNumber="1" x="-218.850" y="186.557" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R261" circuitNumber="2" x="-82.710" y="186.557" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R261" circuitNumber="3" x="-218.850" y="81.757" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R261" circuitNumber="4" x="-82.710" y="81.757" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R286" circuitNumber="1" x="-222.914" y="198.114" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R286" circuitNumber="2" x="-86.774" y="198.114" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R286" circuitNumber="3" x="-222.914" y="93.314" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R286" circuitNumber="4" x="-86.774" y="93.314" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R289" circuitNumber="1" x="-241.964" y="199.892" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R289" circuitNumber="2" x="-105.824" y="199.892" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R289" circuitNumber="3" x="-241.964" y="95.092" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R289" circuitNumber="4" x="-105.824" y="95.092" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R293" circuitNumber="1" x="-241.588" y="204.259" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R293" circuitNumber="2" x="-105.448" y="204.259" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R293" circuitNumber="3" x="-241.588" y="99.459" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R293" circuitNumber="4" x="-105.448" y="99.459" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R308" circuitNumber="1" x="-260.506" y="137.228" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R308" circuitNumber="2" x="-124.366" y="137.228" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R308" circuitNumber="3" x="-260.506" y="32.428" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R308" circuitNumber="4" x="-124.366" y="32.428" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R361" circuitNumber="1" x="-223.138" y="189.859" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R361" circuitNumber="2" x="-86.998" y="189.859" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R361" circuitNumber="3" x="-223.138" y="85.059" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R361" circuitNumber="4" x="-86.998" y="85.059" z="0.000" rz="270.00"/>
		<Component partNumber="510-501859" refDes="R133" circuitNumber="1" x="-256.758" y="161.909" z="0.000" rz="180.00"/>
		<Component partNumber="510-501859" refDes="R133" circuitNumber="2" x="-120.618" y="161.909" z="0.000" rz="180.00"/>
		<Component partNumber="510-501859" refDes="R133" circuitNumber="3" x="-256.758" y="57.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-501859" refDes="R133" circuitNumber="4" x="-120.618" y="57.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R259" circuitNumber="1" x="-241.710" y="185.541" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R259" circuitNumber="2" x="-105.570" y="185.541" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R259" circuitNumber="3" x="-241.710" y="80.741" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R259" circuitNumber="4" x="-105.570" y="80.741" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R260" circuitNumber="1" x="-239.547" y="186.303" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R260" circuitNumber="2" x="-103.407" y="186.303" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R260" circuitNumber="3" x="-239.547" y="81.503" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R260" circuitNumber="4" x="-103.407" y="81.503" z="0.000" rz="180.00"/>
		<Component partNumber="510-502230" refDes="R233" circuitNumber="1" x="-260.887" y="146.854" z="0.000" rz="180.00"/>
		<Component partNumber="510-502230" refDes="R233" circuitNumber="2" x="-124.747" y="146.854" z="0.000" rz="180.00"/>
		<Component partNumber="510-502230" refDes="R233" circuitNumber="3" x="-260.887" y="42.054" z="0.000" rz="180.00"/>
		<Component partNumber="510-502230" refDes="R233" circuitNumber="4" x="-124.747" y="42.054" z="0.000" rz="180.00"/>
		<Component partNumber="511-500221" refDes="C659" circuitNumber="1" x="-243.508" y="200.009" z="0.000" rz="180.00"/>
		<Component partNumber="511-500221" refDes="C659" circuitNumber="2" x="-107.368" y="200.009" z="0.000" rz="180.00"/>
		<Component partNumber="511-500221" refDes="C659" circuitNumber="3" x="-243.508" y="95.209" z="0.000" rz="180.00"/>
		<Component partNumber="511-500221" refDes="C659" circuitNumber="4" x="-107.368" y="95.209" z="0.000" rz="180.00"/>
		<Component partNumber="511-500253" refDes="C613" circuitNumber="1" x="-249.607" y="188.335" z="0.000" rz="180.00"/>
		<Component partNumber="511-500253" refDes="C613" circuitNumber="2" x="-113.467" y="188.335" z="0.000" rz="180.00"/>
		<Component partNumber="511-500253" refDes="C613" circuitNumber="3" x="-249.607" y="83.535" z="0.000" rz="180.00"/>
		<Component partNumber="511-500253" refDes="C613" circuitNumber="4" x="-113.467" y="83.535" z="0.000" rz="180.00"/>
		<Component partNumber="511-500556" refDes="C477" circuitNumber="1" x="-257.838" y="122.809" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C477" circuitNumber="2" x="-121.698" y="122.809" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C477" circuitNumber="3" x="-257.838" y="18.009" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C477" circuitNumber="4" x="-121.698" y="18.009" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C614" circuitNumber="1" x="-249.607" y="188.970" z="0.000" rz="180.00"/>
		<Component partNumber="511-500556" refDes="C614" circuitNumber="2" x="-113.467" y="188.970" z="0.000" rz="180.00"/>
		<Component partNumber="511-500556" refDes="C614" circuitNumber="3" x="-249.607" y="84.170" z="0.000" rz="180.00"/>
		<Component partNumber="511-500556" refDes="C614" circuitNumber="4" x="-113.467" y="84.170" z="0.000" rz="180.00"/>
		<Component partNumber="511-500557" refDes="C546" circuitNumber="1" x="-263.185" y="155.008" z="0.000" rz="0.00"/>
		<Component partNumber="511-500557" refDes="C546" circuitNumber="2" x="-127.045" y="155.008" z="0.000" rz="0.00"/>
		<Component partNumber="511-500557" refDes="C546" circuitNumber="3" x="-263.185" y="50.208" z="0.000" rz="0.00"/>
		<Component partNumber="511-500557" refDes="C546" circuitNumber="4" x="-127.045" y="50.208" z="0.000" rz="0.00"/>
		<Component partNumber="511-500561" refDes="C653" circuitNumber="1" x="-224.692" y="199.130" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C653" circuitNumber="2" x="-88.552" y="199.130" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C653" circuitNumber="3" x="-224.692" y="94.330" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C653" circuitNumber="4" x="-88.552" y="94.330" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C479" circuitNumber="1" x="-259.078" y="123.411" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C479" circuitNumber="2" x="-122.938" y="123.411" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C479" circuitNumber="3" x="-259.078" y="18.611" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C479" circuitNumber="4" x="-122.938" y="18.611" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C583" circuitNumber="1" x="-241.202" y="169.920" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C583" circuitNumber="2" x="-105.062" y="169.920" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C583" circuitNumber="3" x="-241.202" y="65.120" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C583" circuitNumber="4" x="-105.062" y="65.120" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C611" circuitNumber="1" x="-226.089" y="184.017" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C611" circuitNumber="2" x="-89.949" y="184.017" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C611" circuitNumber="3" x="-226.089" y="79.217" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C611" circuitNumber="4" x="-89.949" y="79.217" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C472" circuitNumber="1" x="-160.660" y="117.088" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C472" circuitNumber="2" x="-24.520" y="117.088" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C472" circuitNumber="3" x="-160.660" y="12.288" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C472" circuitNumber="4" x="-24.520" y="12.288" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C474" circuitNumber="1" x="-175.392" y="117.088" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C474" circuitNumber="2" x="-39.252" y="117.088" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C474" circuitNumber="3" x="-175.392" y="12.288" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C474" circuitNumber="4" x="-39.252" y="12.288" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C476" circuitNumber="1" x="-188.219" y="117.088" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C476" circuitNumber="2" x="-52.079" y="117.088" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C476" circuitNumber="3" x="-188.219" y="12.288" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C476" circuitNumber="4" x="-52.079" y="12.288" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C483" circuitNumber="1" x="-257.839" y="125.851" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C483" circuitNumber="2" x="-121.699" y="125.851" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C483" circuitNumber="3" x="-257.839" y="21.051" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C483" circuitNumber="4" x="-121.699" y="21.051" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C492" circuitNumber="1" x="-155.199" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C492" circuitNumber="2" x="-19.059" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C492" circuitNumber="3" x="-155.199" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C492" circuitNumber="4" x="-19.059" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C496" circuitNumber="1" x="-170.566" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C496" circuitNumber="2" x="-34.426" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C496" circuitNumber="3" x="-170.566" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C496" circuitNumber="4" x="-34.426" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C497" circuitNumber="1" x="-174.249" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C497" circuitNumber="2" x="-38.109" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C497" circuitNumber="3" x="-174.249" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C497" circuitNumber="4" x="-38.109" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C501" circuitNumber="1" x="-189.616" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C501" circuitNumber="2" x="-53.476" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C501" circuitNumber="3" x="-189.616" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C501" circuitNumber="4" x="-53.476" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C505" circuitNumber="1" x="-138.054" y="134.888" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C505" circuitNumber="2" x="-1.914" y="134.888" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C505" circuitNumber="3" x="-138.054" y="30.088" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C505" circuitNumber="4" x="-1.914" y="30.088" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C507" circuitNumber="1" x="-247.933" y="137.535" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C507" circuitNumber="2" x="-111.793" y="137.535" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C507" circuitNumber="3" x="-247.933" y="32.735" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C507" circuitNumber="4" x="-111.793" y="32.735" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C508" circuitNumber="1" x="-249.965" y="137.535" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C508" circuitNumber="2" x="-113.825" y="137.535" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C508" circuitNumber="3" x="-249.965" y="32.735" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C508" circuitNumber="4" x="-113.825" y="32.735" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C509" circuitNumber="1" x="-251.870" y="137.535" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C509" circuitNumber="2" x="-115.730" y="137.535" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C509" circuitNumber="3" x="-251.870" y="32.735" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C509" circuitNumber="4" x="-115.730" y="32.735" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C514" circuitNumber="1" x="-138.154" y="145.429" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C514" circuitNumber="2" x="-2.014" y="145.429" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C514" circuitNumber="3" x="-138.154" y="40.629" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C514" circuitNumber="4" x="-2.014" y="40.629" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C515" circuitNumber="1" x="-187.457" y="147.441" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C515" circuitNumber="2" x="-51.317" y="147.441" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C515" circuitNumber="3" x="-187.457" y="42.641" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C515" circuitNumber="4" x="-51.317" y="42.641" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C518" circuitNumber="1" x="-156.850" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C518" circuitNumber="2" x="-20.710" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C518" circuitNumber="3" x="-156.850" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C518" circuitNumber="4" x="-20.710" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C521" circuitNumber="1" x="-175.773" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C521" circuitNumber="2" x="-39.633" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C521" circuitNumber="3" x="-175.773" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C521" circuitNumber="4" x="-39.633" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C527" circuitNumber="1" x="-247.933" y="149.092" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C527" circuitNumber="2" x="-111.793" y="149.092" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C527" circuitNumber="3" x="-247.933" y="44.292" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C527" circuitNumber="4" x="-111.793" y="44.292" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C528" circuitNumber="1" x="-249.965" y="149.092" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C528" circuitNumber="2" x="-113.825" y="149.092" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C528" circuitNumber="3" x="-249.965" y="44.292" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C528" circuitNumber="4" x="-113.825" y="44.292" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C529" circuitNumber="1" x="-251.870" y="149.092" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C529" circuitNumber="2" x="-115.730" y="149.092" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C529" circuitNumber="3" x="-251.870" y="44.292" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C529" circuitNumber="4" x="-115.730" y="44.292" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C531" circuitNumber="1" x="-255.934" y="149.092" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C531" circuitNumber="2" x="-119.794" y="149.092" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C531" circuitNumber="3" x="-255.934" y="44.292" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C531" circuitNumber="4" x="-119.794" y="44.292" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C538" circuitNumber="1" x="-138.154" y="153.664" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C538" circuitNumber="2" x="-2.014" y="153.664" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C538" circuitNumber="3" x="-138.154" y="48.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C538" circuitNumber="4" x="-2.014" y="48.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C550" circuitNumber="1" x="-243.234" y="160.522" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C550" circuitNumber="2" x="-107.094" y="160.522" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C550" circuitNumber="3" x="-243.234" y="55.722" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C550" circuitNumber="4" x="-107.094" y="55.722" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C551" circuitNumber="1" x="-246.917" y="160.522" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C551" circuitNumber="2" x="-110.777" y="160.522" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C551" circuitNumber="3" x="-246.917" y="55.722" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C551" circuitNumber="4" x="-110.777" y="55.722" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C552" circuitNumber="1" x="-248.314" y="160.522" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C552" circuitNumber="2" x="-112.174" y="160.522" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C552" circuitNumber="3" x="-248.314" y="55.722" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C552" circuitNumber="4" x="-112.174" y="55.722" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C560" circuitNumber="1" x="-155.199" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C560" circuitNumber="2" x="-19.059" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C560" circuitNumber="3" x="-155.199" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C560" circuitNumber="4" x="-19.059" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C564" circuitNumber="1" x="-170.566" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C564" circuitNumber="2" x="-34.426" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C564" circuitNumber="3" x="-170.566" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C564" circuitNumber="4" x="-34.426" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C565" circuitNumber="1" x="-174.122" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C565" circuitNumber="2" x="-37.982" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C565" circuitNumber="3" x="-174.122" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C565" circuitNumber="4" x="-37.982" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C569" circuitNumber="1" x="-189.616" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C569" circuitNumber="2" x="-53.476" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C569" circuitNumber="3" x="-189.616" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C569" circuitNumber="4" x="-53.476" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C578" circuitNumber="1" x="-138.054" y="165.602" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C578" circuitNumber="2" x="-1.914" y="165.602" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C578" circuitNumber="3" x="-138.054" y="60.802" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C578" circuitNumber="4" x="-1.914" y="60.802" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C580" circuitNumber="1" x="-226.089" y="167.507" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C580" circuitNumber="2" x="-89.949" y="167.507" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C580" circuitNumber="3" x="-226.089" y="62.707" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C580" circuitNumber="4" x="-89.949" y="62.707" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C581" circuitNumber="1" x="-153.040" y="168.015" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C581" circuitNumber="2" x="-16.900" y="168.015" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C581" circuitNumber="3" x="-153.040" y="63.215" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C581" circuitNumber="4" x="-16.900" y="63.215" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C582" circuitNumber="1" x="-226.089" y="169.793" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C582" circuitNumber="2" x="-89.949" y="169.793" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C582" circuitNumber="3" x="-226.089" y="64.993" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C582" circuitNumber="4" x="-89.949" y="64.993" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C586" circuitNumber="1" x="-243.234" y="172.079" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C586" circuitNumber="2" x="-107.094" y="172.079" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C586" circuitNumber="3" x="-243.234" y="67.279" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C586" circuitNumber="4" x="-107.094" y="67.279" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C588" circuitNumber="1" x="-247.298" y="172.079" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C588" circuitNumber="2" x="-111.158" y="172.079" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C588" circuitNumber="3" x="-247.298" y="67.279" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C588" circuitNumber="4" x="-111.158" y="67.279" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C590" circuitNumber="1" x="-251.235" y="172.079" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C590" circuitNumber="2" x="-115.095" y="172.079" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C590" circuitNumber="3" x="-251.235" y="67.279" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C590" circuitNumber="4" x="-115.095" y="67.279" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C592" circuitNumber="1" x="-210.722" y="172.206" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C592" circuitNumber="2" x="-74.582" y="172.206" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C592" circuitNumber="3" x="-210.722" y="67.406" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C592" circuitNumber="4" x="-74.582" y="67.406" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C593" circuitNumber="1" x="-241.456" y="172.333" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C593" circuitNumber="2" x="-105.316" y="172.333" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C593" circuitNumber="3" x="-241.456" y="67.533" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C593" circuitNumber="4" x="-105.316" y="67.533" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C597" circuitNumber="1" x="-156.596" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C597" circuitNumber="2" x="-20.456" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C597" circuitNumber="3" x="-156.596" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C597" circuitNumber="4" x="-20.456" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C600" circuitNumber="1" x="-175.646" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C600" circuitNumber="2" x="-39.506" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C600" circuitNumber="3" x="-175.646" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C600" circuitNumber="4" x="-39.506" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C606" circuitNumber="1" x="-226.089" y="181.858" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C606" circuitNumber="2" x="-89.949" y="181.858" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C606" circuitNumber="3" x="-226.089" y="77.058" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C606" circuitNumber="4" x="-89.949" y="77.058" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C608" circuitNumber="1" x="-246.409" y="183.636" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C608" circuitNumber="2" x="-110.269" y="183.636" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C608" circuitNumber="3" x="-246.409" y="78.836" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C608" circuitNumber="4" x="-110.269" y="78.836" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C610" circuitNumber="1" x="-251.870" y="183.636" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C610" circuitNumber="2" x="-115.730" y="183.636" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C610" circuitNumber="3" x="-251.870" y="78.836" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C610" circuitNumber="4" x="-115.730" y="78.836" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C631" circuitNumber="1" x="-155.199" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C631" circuitNumber="2" x="-19.059" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C631" circuitNumber="3" x="-155.199" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C631" circuitNumber="4" x="-19.059" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C635" circuitNumber="1" x="-170.389" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C635" circuitNumber="2" x="-34.249" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C635" circuitNumber="3" x="-170.389" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C635" circuitNumber="4" x="-34.249" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C636" circuitNumber="1" x="-174.288" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C636" circuitNumber="2" x="-38.148" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C636" circuitNumber="3" x="-174.288" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C636" circuitNumber="4" x="-38.148" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C640" circuitNumber="1" x="-189.616" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C640" circuitNumber="2" x="-53.476" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C640" circuitNumber="3" x="-189.616" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C640" circuitNumber="4" x="-53.476" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C682" circuitNumber="1" x="-156.850" y="208.933" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C682" circuitNumber="2" x="-20.710" y="208.933" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C682" circuitNumber="3" x="-156.850" y="104.133" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C682" circuitNumber="4" x="-20.710" y="104.133" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C685" circuitNumber="1" x="-174.630" y="208.933" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C685" circuitNumber="2" x="-38.490" y="208.933" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C685" circuitNumber="3" x="-174.630" y="104.133" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C685" circuitNumber="4" x="-38.490" y="104.133" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C687" circuitNumber="1" x="-187.711" y="208.933" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C687" circuitNumber="2" x="-51.571" y="208.933" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C687" circuitNumber="3" x="-187.711" y="104.133" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C687" circuitNumber="4" x="-51.571" y="104.133" z="0.000" rz="180.00"/>
		<Component partNumber="511-500595" refDes="C542" circuitNumber="1" x="-268.038" y="151.409" z="0.000" rz="180.00"/>
		<Component partNumber="511-500595" refDes="C542" circuitNumber="2" x="-131.898" y="151.409" z="0.000" rz="180.00"/>
		<Component partNumber="511-500595" refDes="C542" circuitNumber="3" x="-268.038" y="46.609" z="0.000" rz="180.00"/>
		<Component partNumber="511-500595" refDes="C542" circuitNumber="4" x="-131.898" y="46.609" z="0.000" rz="180.00"/>
		<Component partNumber="511-500595" refDes="C739" circuitNumber="1" x="-239.344" y="127.177" z="0.000" rz="270.00"/>
		<Component partNumber="511-500595" refDes="C739" circuitNumber="2" x="-103.204" y="127.177" z="0.000" rz="270.00"/>
		<Component partNumber="511-500595" refDes="C739" circuitNumber="3" x="-239.344" y="22.377" z="0.000" rz="270.00"/>
		<Component partNumber="511-500595" refDes="C739" circuitNumber="4" x="-103.204" y="22.377" z="0.000" rz="270.00"/>
		<Component partNumber="511-500595" refDes="C740" circuitNumber="1" x="-239.979" y="127.177" z="0.000" rz="270.00"/>
		<Component partNumber="511-500595" refDes="C740" circuitNumber="2" x="-103.839" y="127.177" z="0.000" rz="270.00"/>
		<Component partNumber="511-500595" refDes="C740" circuitNumber="3" x="-239.979" y="22.377" z="0.000" rz="270.00"/>
		<Component partNumber="511-500595" refDes="C740" circuitNumber="4" x="-103.839" y="22.377" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C422" circuitNumber="1" x="-217.584" y="187.319" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C422" circuitNumber="2" x="-81.444" y="187.319" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C422" circuitNumber="3" x="-217.584" y="82.519" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C422" circuitNumber="4" x="-81.444" y="82.519" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C478" circuitNumber="1" x="-257.838" y="123.409" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C478" circuitNumber="2" x="-121.698" y="123.409" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C478" circuitNumber="3" x="-257.838" y="18.609" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C478" circuitNumber="4" x="-121.698" y="18.609" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C491" circuitNumber="1" x="-268.253" y="131.613" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C491" circuitNumber="2" x="-132.113" y="131.613" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C491" circuitNumber="3" x="-268.253" y="26.813" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C491" circuitNumber="4" x="-132.113" y="26.813" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C493" circuitNumber="1" x="-158.755" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C493" circuitNumber="2" x="-22.615" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C493" circuitNumber="3" x="-158.755" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C493" circuitNumber="4" x="-22.615" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C495" circuitNumber="1" x="-165.359" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C495" circuitNumber="2" x="-29.219" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C495" circuitNumber="3" x="-165.359" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C495" circuitNumber="4" x="-29.219" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C498" circuitNumber="1" x="-177.805" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C498" circuitNumber="2" x="-41.665" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C498" circuitNumber="3" x="-177.805" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C498" circuitNumber="4" x="-41.665" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C500" circuitNumber="1" x="-184.409" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C500" circuitNumber="2" x="-48.269" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C500" circuitNumber="3" x="-184.409" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C500" circuitNumber="4" x="-48.269" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C502" circuitNumber="1" x="-168.153" y="132.455" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C502" circuitNumber="2" x="-32.013" y="132.455" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C502" circuitNumber="3" x="-168.153" y="27.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C502" circuitNumber="4" x="-32.013" y="27.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C503" circuitNumber="1" x="-187.203" y="132.455" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C503" circuitNumber="2" x="-51.063" y="132.455" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C503" circuitNumber="3" x="-187.203" y="27.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C503" circuitNumber="4" x="-51.063" y="27.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C510" circuitNumber="1" x="-253.902" y="137.535" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C510" circuitNumber="2" x="-117.762" y="137.535" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C510" circuitNumber="3" x="-253.902" y="32.735" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C510" circuitNumber="4" x="-117.762" y="32.735" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C511" circuitNumber="1" x="-255.934" y="137.535" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C511" circuitNumber="2" x="-119.794" y="137.535" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C511" circuitNumber="3" x="-255.934" y="32.735" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C511" circuitNumber="4" x="-119.794" y="32.735" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C530" circuitNumber="1" x="-253.902" y="149.092" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C530" circuitNumber="2" x="-117.762" y="149.092" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C530" circuitNumber="3" x="-253.902" y="44.292" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C530" circuitNumber="4" x="-117.762" y="44.292" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C540" circuitNumber="1" x="-153.421" y="153.918" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C540" circuitNumber="2" x="-17.281" y="153.918" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C540" circuitNumber="3" x="-153.421" y="49.118" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C540" circuitNumber="4" x="-17.281" y="49.118" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C545" circuitNumber="1" x="-261.741" y="155.088" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C545" circuitNumber="2" x="-125.601" y="155.088" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C545" circuitNumber="3" x="-261.741" y="50.288" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C545" circuitNumber="4" x="-125.601" y="50.288" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C549" circuitNumber="1" x="-268.288" y="155.368" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C549" circuitNumber="2" x="-132.148" y="155.368" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C549" circuitNumber="3" x="-268.288" y="50.568" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C549" circuitNumber="4" x="-132.148" y="50.568" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C554" circuitNumber="1" x="-252.251" y="160.522" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C554" circuitNumber="2" x="-116.111" y="160.522" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C554" circuitNumber="3" x="-252.251" y="55.722" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C554" circuitNumber="4" x="-116.111" y="55.722" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C555" circuitNumber="1" x="-254.283" y="160.522" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C555" circuitNumber="2" x="-118.143" y="160.522" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C555" circuitNumber="3" x="-254.283" y="55.722" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C555" circuitNumber="4" x="-118.143" y="55.722" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C561" circuitNumber="1" x="-158.755" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C561" circuitNumber="2" x="-22.615" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C561" circuitNumber="3" x="-158.755" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C561" circuitNumber="4" x="-22.615" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C563" circuitNumber="1" x="-165.359" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C563" circuitNumber="2" x="-29.219" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C563" circuitNumber="3" x="-165.359" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C563" circuitNumber="4" x="-29.219" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C566" circuitNumber="1" x="-177.805" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C566" circuitNumber="2" x="-41.665" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C566" circuitNumber="3" x="-177.805" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C566" circuitNumber="4" x="-41.665" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C568" circuitNumber="1" x="-184.409" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C568" circuitNumber="2" x="-48.269" y="163.062" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C568" circuitNumber="3" x="-184.409" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C568" circuitNumber="4" x="-48.269" y="58.262" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C576" circuitNumber="1" x="-168.153" y="163.443" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C576" circuitNumber="2" x="-32.013" y="163.443" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C576" circuitNumber="3" x="-168.153" y="58.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C576" circuitNumber="4" x="-32.013" y="58.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C577" circuitNumber="1" x="-187.203" y="163.443" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C577" circuitNumber="2" x="-51.063" y="163.443" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C577" circuitNumber="3" x="-187.203" y="58.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C577" circuitNumber="4" x="-51.063" y="58.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C584" circuitNumber="1" x="-225.708" y="171.190" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C584" circuitNumber="2" x="-89.568" y="171.190" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C584" circuitNumber="3" x="-225.708" y="66.390" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C584" circuitNumber="4" x="-89.568" y="66.390" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C587" circuitNumber="1" x="-245.266" y="172.079" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C587" circuitNumber="2" x="-109.126" y="172.079" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C587" circuitNumber="3" x="-245.266" y="67.279" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C587" circuitNumber="4" x="-109.126" y="67.279" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C589" circuitNumber="1" x="-249.330" y="172.079" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C589" circuitNumber="2" x="-113.190" y="172.079" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C589" circuitNumber="3" x="-249.330" y="67.279" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C589" circuitNumber="4" x="-113.190" y="67.279" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C594" circuitNumber="1" x="-226.089" y="173.984" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C594" circuitNumber="2" x="-89.949" y="173.984" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C594" circuitNumber="3" x="-226.089" y="69.184" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C594" circuitNumber="4" x="-89.949" y="69.184" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C604" circuitNumber="1" x="-226.089" y="180.588" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C604" circuitNumber="2" x="-89.949" y="180.588" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C604" circuitNumber="3" x="-226.089" y="75.788" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C604" circuitNumber="4" x="-89.949" y="75.788" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C607" circuitNumber="1" x="-242.980" y="183.509" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C607" circuitNumber="2" x="-106.840" y="183.509" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C607" circuitNumber="3" x="-242.980" y="78.709" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C607" circuitNumber="4" x="-106.840" y="78.709" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C609" circuitNumber="1" x="-249.203" y="183.636" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C609" circuitNumber="2" x="-113.063" y="183.636" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C609" circuitNumber="3" x="-249.203" y="78.836" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C609" circuitNumber="4" x="-113.063" y="78.836" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C618" circuitNumber="1" x="-240.694" y="189.732" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C618" circuitNumber="2" x="-104.554" y="189.732" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C618" circuitNumber="3" x="-240.694" y="84.932" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C618" circuitNumber="4" x="-104.554" y="84.932" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C623" circuitNumber="1" x="-242.503" y="192.272" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C623" circuitNumber="2" x="-106.363" y="192.272" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C623" circuitNumber="3" x="-242.503" y="87.472" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C623" circuitNumber="4" x="-106.363" y="87.472" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C628" circuitNumber="1" x="-216.488" y="195.759" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C628" circuitNumber="2" x="-80.348" y="195.759" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C628" circuitNumber="3" x="-216.488" y="90.959" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C628" circuitNumber="4" x="-80.348" y="90.959" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C632" circuitNumber="1" x="-158.755" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C632" circuitNumber="2" x="-22.615" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C632" circuitNumber="3" x="-158.755" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C632" circuitNumber="4" x="-22.615" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C634" circuitNumber="1" x="-165.359" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C634" circuitNumber="2" x="-29.219" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C634" circuitNumber="3" x="-165.359" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C634" circuitNumber="4" x="-29.219" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C637" circuitNumber="1" x="-177.805" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C637" circuitNumber="2" x="-41.665" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C637" circuitNumber="3" x="-177.805" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C637" circuitNumber="4" x="-41.665" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C639" circuitNumber="1" x="-184.409" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C639" circuitNumber="2" x="-48.269" y="194.050" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C639" circuitNumber="3" x="-184.409" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C639" circuitNumber="4" x="-48.269" y="89.250" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C641" circuitNumber="1" x="-168.153" y="194.431" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C641" circuitNumber="2" x="-32.013" y="194.431" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C641" circuitNumber="3" x="-168.153" y="89.631" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C641" circuitNumber="4" x="-32.013" y="89.631" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C642" circuitNumber="1" x="-187.203" y="194.431" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C642" circuitNumber="2" x="-51.063" y="194.431" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C642" circuitNumber="3" x="-187.203" y="89.631" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C642" circuitNumber="4" x="-51.063" y="89.631" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C643" circuitNumber="1" x="-217.338" y="194.109" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C643" circuitNumber="2" x="-81.198" y="194.109" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C643" circuitNumber="3" x="-217.338" y="89.309" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C643" circuitNumber="4" x="-81.198" y="89.309" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C644" circuitNumber="1" x="-233.709" y="195.263" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C644" circuitNumber="2" x="-97.569" y="195.263" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C644" circuitNumber="3" x="-233.709" y="90.463" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C644" circuitNumber="4" x="-97.569" y="90.463" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C645" circuitNumber="1" x="-237.646" y="195.263" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C645" circuitNumber="2" x="-101.506" y="195.263" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C645" circuitNumber="3" x="-237.646" y="90.463" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C645" circuitNumber="4" x="-101.506" y="90.463" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C646" circuitNumber="1" x="-220.288" y="196.459" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C646" circuitNumber="2" x="-84.148" y="196.459" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C646" circuitNumber="3" x="-220.288" y="91.659" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C646" circuitNumber="4" x="-84.148" y="91.659" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C648" circuitNumber="1" x="-223.676" y="197.098" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C648" circuitNumber="2" x="-87.536" y="197.098" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C648" circuitNumber="3" x="-223.676" y="92.298" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C648" circuitNumber="4" x="-87.536" y="92.298" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C650" circuitNumber="1" x="-243.977" y="197.225" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C650" circuitNumber="2" x="-107.837" y="197.225" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C650" circuitNumber="3" x="-243.977" y="92.425" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C650" circuitNumber="4" x="-107.837" y="92.425" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C651" circuitNumber="1" x="-240.821" y="198.622" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C651" circuitNumber="2" x="-104.681" y="198.622" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C651" circuitNumber="3" x="-240.821" y="93.822" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C651" circuitNumber="4" x="-104.681" y="93.822" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C656" circuitNumber="1" x="-227.359" y="201.514" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C656" circuitNumber="2" x="-91.219" y="201.514" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C656" circuitNumber="3" x="-227.359" y="96.714" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C656" circuitNumber="4" x="-91.219" y="96.714" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C657" circuitNumber="1" x="-228.688" y="201.514" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C657" circuitNumber="2" x="-92.548" y="201.514" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C657" circuitNumber="3" x="-228.688" y="96.714" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C657" circuitNumber="4" x="-92.548" y="96.714" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C658" circuitNumber="1" x="-230.138" y="202.059" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C658" circuitNumber="2" x="-93.998" y="202.059" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C658" circuitNumber="3" x="-230.138" y="97.259" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C658" circuitNumber="4" x="-93.998" y="97.259" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C665" circuitNumber="1" x="-230.915" y="205.353" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C665" circuitNumber="2" x="-94.775" y="205.353" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C665" circuitNumber="3" x="-230.915" y="100.553" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C665" circuitNumber="4" x="-94.775" y="100.553" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C666" circuitNumber="1" x="-236.376" y="205.480" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C666" circuitNumber="2" x="-100.236" y="205.480" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C666" circuitNumber="3" x="-236.376" y="100.680" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C666" circuitNumber="4" x="-100.236" y="100.680" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C667" circuitNumber="1" x="-233.455" y="205.607" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C667" circuitNumber="2" x="-97.315" y="205.607" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C667" circuitNumber="3" x="-233.455" y="100.807" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C667" circuitNumber="4" x="-97.315" y="100.807" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C672" circuitNumber="1" x="-215.040" y="207.004" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C672" circuitNumber="2" x="-78.900" y="207.004" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C672" circuitNumber="3" x="-215.040" y="102.204" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C672" circuitNumber="4" x="-78.900" y="102.204" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C679" circuitNumber="1" x="-225.454" y="208.274" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C679" circuitNumber="2" x="-89.314" y="208.274" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C679" circuitNumber="3" x="-225.454" y="103.474" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C679" circuitNumber="4" x="-89.314" y="103.474" z="0.000" rz="90.00"/>
		<Component partNumber="511-500616" refDes="C647" circuitNumber="1" x="-221.288" y="196.859" z="0.000" rz="180.00"/>
		<Component partNumber="511-500616" refDes="C647" circuitNumber="2" x="-85.148" y="196.859" z="0.000" rz="180.00"/>
		<Component partNumber="511-500616" refDes="C647" circuitNumber="3" x="-221.288" y="92.059" z="0.000" rz="180.00"/>
		<Component partNumber="511-500616" refDes="C647" circuitNumber="4" x="-85.148" y="92.059" z="0.000" rz="180.00"/>
		<Component partNumber="511-500643" refDes="C660" circuitNumber="1" x="-241.437" y="202.251" z="0.000" rz="90.00"/>
		<Component partNumber="511-500643" refDes="C660" circuitNumber="2" x="-105.297" y="202.251" z="0.000" rz="90.00"/>
		<Component partNumber="511-500643" refDes="C660" circuitNumber="3" x="-241.437" y="97.451" z="0.000" rz="90.00"/>
		<Component partNumber="511-500643" refDes="C660" circuitNumber="4" x="-105.297" y="97.451" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C21" circuitNumber="1" x="-238.044" y="127.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C21" circuitNumber="2" x="-101.904" y="127.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C21" circuitNumber="3" x="-238.044" y="22.377" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C21" circuitNumber="4" x="-101.904" y="22.377" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C22" circuitNumber="1" x="-237.409" y="127.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C22" circuitNumber="2" x="-101.269" y="127.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C22" circuitNumber="3" x="-237.409" y="22.377" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C22" circuitNumber="4" x="-101.269" y="22.377" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C25" circuitNumber="1" x="-236.109" y="127.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C25" circuitNumber="2" x="-99.969" y="127.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C25" circuitNumber="3" x="-236.109" y="22.377" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C25" circuitNumber="4" x="-99.969" y="22.377" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C26" circuitNumber="1" x="-235.474" y="127.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C26" circuitNumber="2" x="-99.334" y="127.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C26" circuitNumber="3" x="-235.474" y="22.377" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C26" circuitNumber="4" x="-99.334" y="22.377" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C485" circuitNumber="1" x="-228.756" y="128.899" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C485" circuitNumber="2" x="-92.616" y="128.899" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C485" circuitNumber="3" x="-228.756" y="24.099" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C485" circuitNumber="4" x="-92.616" y="24.099" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C486" circuitNumber="1" x="-229.391" y="128.899" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C486" circuitNumber="2" x="-93.251" y="128.899" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C486" circuitNumber="3" x="-229.391" y="24.099" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C486" circuitNumber="4" x="-93.251" y="24.099" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C487" circuitNumber="1" x="-232.439" y="128.899" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C487" circuitNumber="2" x="-96.299" y="128.899" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C487" circuitNumber="3" x="-232.439" y="24.099" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C487" circuitNumber="4" x="-96.299" y="24.099" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C488" circuitNumber="1" x="-233.074" y="128.899" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C488" circuitNumber="2" x="-96.934" y="128.899" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C488" circuitNumber="3" x="-233.074" y="24.099" z="0.000" rz="90.00"/>
		<Component partNumber="511-500841" refDes="C488" circuitNumber="4" x="-96.934" y="24.099" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C626" circuitNumber="1" x="-235.741" y="192.907" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C626" circuitNumber="2" x="-99.601" y="192.907" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C626" circuitNumber="3" x="-235.741" y="88.107" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C626" circuitNumber="4" x="-99.601" y="88.107" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C654" circuitNumber="1" x="-227.613" y="200.441" z="0.000" rz="0.00"/>
		<Component partNumber="511-500866" refDes="C654" circuitNumber="2" x="-91.473" y="200.441" z="0.000" rz="0.00"/>
		<Component partNumber="511-500866" refDes="C654" circuitNumber="3" x="-227.613" y="95.641" z="0.000" rz="0.00"/>
		<Component partNumber="511-500866" refDes="C654" circuitNumber="4" x="-91.473" y="95.641" z="0.000" rz="0.00"/>
		<Component partNumber="511-500866" refDes="C675" circuitNumber="1" x="-244.812" y="207.512" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C675" circuitNumber="2" x="-108.672" y="207.512" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C675" circuitNumber="3" x="-244.812" y="102.712" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C675" circuitNumber="4" x="-108.672" y="102.712" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C676" circuitNumber="1" x="-246.413" y="207.512" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C676" circuitNumber="2" x="-110.273" y="207.512" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C676" circuitNumber="3" x="-246.413" y="102.712" z="0.000" rz="270.00"/>
		<Component partNumber="511-500866" refDes="C676" circuitNumber="4" x="-110.273" y="102.712" z="0.000" rz="270.00"/>
		<Component partNumber="511-500895" refDes="C484" circuitNumber="1" x="-217.638" y="128.009" z="0.000" rz="0.00"/>
		<Component partNumber="511-500895" refDes="C484" circuitNumber="2" x="-81.498" y="128.009" z="0.000" rz="0.00"/>
		<Component partNumber="511-500895" refDes="C484" circuitNumber="3" x="-217.638" y="23.209" z="0.000" rz="0.00"/>
		<Component partNumber="511-500895" refDes="C484" circuitNumber="4" x="-81.498" y="23.209" z="0.000" rz="0.00"/>
		<Component partNumber="511-500916" refDes="C663" circuitNumber="1" x="-215.061" y="205.099" z="0.000" rz="270.00"/>
		<Component partNumber="511-500916" refDes="C663" circuitNumber="2" x="-78.921" y="205.099" z="0.000" rz="270.00"/>
		<Component partNumber="511-500916" refDes="C663" circuitNumber="3" x="-215.061" y="100.299" z="0.000" rz="270.00"/>
		<Component partNumber="511-500916" refDes="C663" circuitNumber="4" x="-78.921" y="100.299" z="0.000" rz="270.00"/>
		<Component partNumber="511-500916" refDes="C664" circuitNumber="1" x="-216.655" y="205.097" z="0.000" rz="270.00"/>
		<Component partNumber="511-500916" refDes="C664" circuitNumber="2" x="-80.515" y="205.097" z="0.000" rz="270.00"/>
		<Component partNumber="511-500916" refDes="C664" circuitNumber="3" x="-216.655" y="100.297" z="0.000" rz="270.00"/>
		<Component partNumber="511-500916" refDes="C664" circuitNumber="4" x="-80.515" y="100.297" z="0.000" rz="270.00"/>
		<Component partNumber="511-500916" refDes="C670" circuitNumber="1" x="-230.915" y="206.623" z="0.000" rz="0.00"/>
		<Component partNumber="511-500916" refDes="C670" circuitNumber="2" x="-94.775" y="206.623" z="0.000" rz="0.00"/>
		<Component partNumber="511-500916" refDes="C670" circuitNumber="3" x="-230.915" y="101.823" z="0.000" rz="0.00"/>
		<Component partNumber="511-500916" refDes="C670" circuitNumber="4" x="-94.775" y="101.823" z="0.000" rz="0.00"/>
		<Component partNumber="511-500916" refDes="C671" circuitNumber="1" x="-237.646" y="206.877" z="0.000" rz="90.00"/>
		<Component partNumber="511-500916" refDes="C671" circuitNumber="2" x="-101.506" y="206.877" z="0.000" rz="90.00"/>
		<Component partNumber="511-500916" refDes="C671" circuitNumber="3" x="-237.646" y="102.077" z="0.000" rz="90.00"/>
		<Component partNumber="511-500916" refDes="C671" circuitNumber="4" x="-101.506" y="102.077" z="0.000" rz="90.00"/>
		<Component partNumber="511-500916" refDes="C680" circuitNumber="1" x="-230.915" y="208.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500916" refDes="C680" circuitNumber="2" x="-94.775" y="208.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500916" refDes="C680" circuitNumber="3" x="-230.915" y="103.474" z="0.000" rz="0.00"/>
		<Component partNumber="511-500916" refDes="C680" circuitNumber="4" x="-94.775" y="103.474" z="0.000" rz="0.00"/>
		<Component partNumber="511-500917" refDes="C624" circuitNumber="1" x="-243.885" y="192.399" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C624" circuitNumber="2" x="-107.745" y="192.399" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C624" circuitNumber="3" x="-243.885" y="87.599" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C624" circuitNumber="4" x="-107.745" y="87.599" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C625" circuitNumber="1" x="-245.933" y="192.399" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C625" circuitNumber="2" x="-109.793" y="192.399" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C625" circuitNumber="3" x="-245.933" y="87.599" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C625" circuitNumber="4" x="-109.793" y="87.599" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C431" circuitNumber="1" x="-233.074" y="190.310" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C431" circuitNumber="2" x="-96.934" y="190.310" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C431" circuitNumber="3" x="-233.074" y="85.510" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C431" circuitNumber="4" x="-96.934" y="85.510" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C506" circuitNumber="1" x="-245.588" y="137.583" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C506" circuitNumber="2" x="-109.448" y="137.583" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C506" circuitNumber="3" x="-245.588" y="32.783" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C506" circuitNumber="4" x="-109.448" y="32.783" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C523" circuitNumber="1" x="-260.914" y="145.046" z="0.000" rz="0.00"/>
		<Component partNumber="511-500918" refDes="C523" circuitNumber="2" x="-124.774" y="145.046" z="0.000" rz="0.00"/>
		<Component partNumber="511-500918" refDes="C523" circuitNumber="3" x="-260.914" y="40.246" z="0.000" rz="0.00"/>
		<Component partNumber="511-500918" refDes="C523" circuitNumber="4" x="-124.774" y="40.246" z="0.000" rz="0.00"/>
		<Component partNumber="511-500918" refDes="C526" circuitNumber="1" x="-245.638" y="149.083" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C526" circuitNumber="2" x="-109.498" y="149.083" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C526" circuitNumber="3" x="-245.638" y="44.283" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C526" circuitNumber="4" x="-109.498" y="44.283" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C537" circuitNumber="1" x="-261.614" y="149.309" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C537" circuitNumber="2" x="-125.474" y="149.309" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C537" circuitNumber="3" x="-261.614" y="44.509" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C537" circuitNumber="4" x="-125.474" y="44.509" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C541" circuitNumber="1" x="-261.614" y="151.426" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C541" circuitNumber="2" x="-125.474" y="151.426" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C541" circuitNumber="3" x="-261.614" y="46.626" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C541" circuitNumber="4" x="-125.474" y="46.626" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C544" circuitNumber="1" x="-261.614" y="153.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C544" circuitNumber="2" x="-125.474" y="153.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C544" circuitNumber="3" x="-261.614" y="48.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C544" circuitNumber="4" x="-125.474" y="48.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C570" circuitNumber="1" x="-223.041" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C570" circuitNumber="2" x="-86.901" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C570" circuitNumber="3" x="-223.041" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C570" circuitNumber="4" x="-86.901" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C571" circuitNumber="1" x="-225.608" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C571" circuitNumber="2" x="-89.468" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C571" circuitNumber="3" x="-225.608" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C571" circuitNumber="4" x="-89.468" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C572" circuitNumber="1" x="-227.660" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C572" circuitNumber="2" x="-91.520" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C572" circuitNumber="3" x="-227.660" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C572" circuitNumber="4" x="-91.520" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C573" circuitNumber="1" x="-229.722" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C573" circuitNumber="2" x="-93.582" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C573" circuitNumber="3" x="-229.722" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C573" circuitNumber="4" x="-93.582" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C574" circuitNumber="1" x="-231.784" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C574" circuitNumber="2" x="-95.644" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C574" circuitNumber="3" x="-231.784" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C574" circuitNumber="4" x="-95.644" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C575" circuitNumber="1" x="-233.836" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C575" circuitNumber="2" x="-97.696" y="163.062" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C575" circuitNumber="3" x="-233.836" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C575" circuitNumber="4" x="-97.696" y="58.262" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C621" circuitNumber="1" x="-238.408" y="190.310" z="0.000" rz="0.00"/>
		<Component partNumber="511-500918" refDes="C621" circuitNumber="2" x="-102.268" y="190.310" z="0.000" rz="0.00"/>
		<Component partNumber="511-500918" refDes="C621" circuitNumber="3" x="-238.408" y="85.510" z="0.000" rz="0.00"/>
		<Component partNumber="511-500918" refDes="C621" circuitNumber="4" x="-102.268" y="85.510" z="0.000" rz="0.00"/>
		<Component partNumber="511-500918" refDes="C668" circuitNumber="1" x="-227.486" y="206.237" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C668" circuitNumber="2" x="-91.346" y="206.237" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C668" circuitNumber="3" x="-227.486" y="101.437" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C668" circuitNumber="4" x="-91.346" y="101.437" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C678" circuitNumber="1" x="-227.486" y="208.285" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C678" circuitNumber="2" x="-91.346" y="208.285" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C678" circuitNumber="3" x="-227.486" y="103.485" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C678" circuitNumber="4" x="-91.346" y="103.485" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C398" circuitNumber="1" x="-221.338" y="187.459" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C398" circuitNumber="2" x="-85.198" y="187.459" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C398" circuitNumber="3" x="-221.338" y="82.659" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C398" circuitNumber="4" x="-85.198" y="82.659" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C494" circuitNumber="1" x="-160.660" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C494" circuitNumber="2" x="-24.520" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C494" circuitNumber="3" x="-160.660" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C494" circuitNumber="4" x="-24.520" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C499" circuitNumber="1" x="-179.710" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C499" circuitNumber="2" x="-43.570" y="132.074" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C499" circuitNumber="3" x="-179.710" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C499" circuitNumber="4" x="-43.570" y="27.274" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C513" circuitNumber="1" x="-153.040" y="138.740" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C513" circuitNumber="2" x="-16.900" y="138.740" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C513" circuitNumber="3" x="-153.040" y="33.940" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C513" circuitNumber="4" x="-16.900" y="33.940" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C519" circuitNumber="1" x="-160.660" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C519" circuitNumber="2" x="-24.520" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C519" circuitNumber="3" x="-160.660" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C519" circuitNumber="4" x="-24.520" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C522" circuitNumber="1" x="-179.710" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C522" circuitNumber="2" x="-43.570" y="147.568" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C522" circuitNumber="3" x="-179.710" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C522" circuitNumber="4" x="-43.570" y="42.768" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C535" circuitNumber="1" x="-263.988" y="148.659" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C535" circuitNumber="2" x="-127.848" y="148.659" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C535" circuitNumber="3" x="-263.988" y="43.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C535" circuitNumber="4" x="-127.848" y="43.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C547" circuitNumber="1" x="-266.558" y="154.909" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C547" circuitNumber="2" x="-130.418" y="154.909" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C547" circuitNumber="3" x="-266.558" y="50.109" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C547" circuitNumber="4" x="-130.418" y="50.109" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C548" circuitNumber="1" x="-153.040" y="158.490" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C548" circuitNumber="2" x="-16.900" y="158.490" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C548" circuitNumber="3" x="-153.040" y="53.690" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C548" circuitNumber="4" x="-16.900" y="53.690" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C562" circuitNumber="1" x="-160.660" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C562" circuitNumber="2" x="-24.520" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C562" circuitNumber="3" x="-160.660" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C562" circuitNumber="4" x="-24.520" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C567" circuitNumber="1" x="-179.710" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C567" circuitNumber="2" x="-43.570" y="163.062" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C567" circuitNumber="3" x="-179.710" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C567" circuitNumber="4" x="-43.570" y="58.262" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C598" circuitNumber="1" x="-160.660" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C598" circuitNumber="2" x="-24.520" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C598" circuitNumber="3" x="-160.660" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C598" circuitNumber="4" x="-24.520" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C601" circuitNumber="1" x="-179.710" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C601" circuitNumber="2" x="-43.570" y="178.556" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C601" circuitNumber="3" x="-179.710" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C601" circuitNumber="4" x="-43.570" y="73.756" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C602" circuitNumber="1" x="-216.038" y="187.459" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C602" circuitNumber="2" x="-79.898" y="187.459" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C602" circuitNumber="3" x="-216.038" y="82.659" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C602" circuitNumber="4" x="-79.898" y="82.659" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C603" circuitNumber="1" x="-226.089" y="178.683" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C603" circuitNumber="2" x="-89.949" y="178.683" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C603" circuitNumber="3" x="-226.089" y="73.883" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C603" circuitNumber="4" x="-89.949" y="73.883" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C633" circuitNumber="1" x="-160.660" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C633" circuitNumber="2" x="-24.520" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C633" circuitNumber="3" x="-160.660" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C633" circuitNumber="4" x="-24.520" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C638" circuitNumber="1" x="-179.710" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C638" circuitNumber="2" x="-43.570" y="194.050" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C638" circuitNumber="3" x="-179.710" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C638" circuitNumber="4" x="-43.570" y="89.250" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C662" circuitNumber="1" x="-240.567" y="204.718" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C662" circuitNumber="2" x="-104.427" y="204.718" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C662" circuitNumber="3" x="-240.567" y="99.918" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C662" circuitNumber="4" x="-104.427" y="99.918" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C669" circuitNumber="1" x="-239.043" y="206.496" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C669" circuitNumber="2" x="-102.903" y="206.496" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C669" circuitNumber="3" x="-239.043" y="101.696" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C669" circuitNumber="4" x="-102.903" y="101.696" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C673" circuitNumber="1" x="-234.979" y="207.131" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C673" circuitNumber="2" x="-98.839" y="207.131" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C673" circuitNumber="3" x="-234.979" y="102.331" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C673" circuitNumber="4" x="-98.839" y="102.331" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C674" circuitNumber="1" x="-236.122" y="207.131" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C674" circuitNumber="2" x="-99.982" y="207.131" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C674" circuitNumber="3" x="-236.122" y="102.331" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C674" circuitNumber="4" x="-99.982" y="102.331" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C677" circuitNumber="1" x="-243.488" y="207.639" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C677" circuitNumber="2" x="-107.348" y="207.639" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C677" circuitNumber="3" x="-243.488" y="102.839" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C677" circuitNumber="4" x="-107.348" y="102.839" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C683" circuitNumber="1" x="-160.660" y="208.883" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C683" circuitNumber="2" x="-24.520" y="208.883" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C683" circuitNumber="3" x="-160.660" y="104.083" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C683" circuitNumber="4" x="-24.520" y="104.083" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C686" circuitNumber="1" x="-179.710" y="208.883" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C686" circuitNumber="2" x="-43.570" y="208.883" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C686" circuitNumber="3" x="-179.710" y="104.083" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C686" circuitNumber="4" x="-43.570" y="104.083" z="0.000" rz="0.00"/>
		<Component partNumber="511-500920" refDes="C661" circuitNumber="1" x="-243.233" y="202.940" z="0.000" rz="90.00"/>
		<Component partNumber="511-500920" refDes="C661" circuitNumber="2" x="-107.093" y="202.940" z="0.000" rz="90.00"/>
		<Component partNumber="511-500920" refDes="C661" circuitNumber="3" x="-243.233" y="98.140" z="0.000" rz="90.00"/>
		<Component partNumber="511-500920" refDes="C661" circuitNumber="4" x="-107.093" y="98.140" z="0.000" rz="90.00"/>
		<Component partNumber="511-500921" refDes="C652" circuitNumber="1" x="-216.691" y="199.130" z="0.000" rz="90.00"/>
		<Component partNumber="511-500921" refDes="C652" circuitNumber="2" x="-80.551" y="199.130" z="0.000" rz="90.00"/>
		<Component partNumber="511-500921" refDes="C652" circuitNumber="3" x="-216.691" y="94.330" z="0.000" rz="90.00"/>
		<Component partNumber="511-500921" refDes="C652" circuitNumber="4" x="-80.551" y="94.330" z="0.000" rz="90.00"/>
		<Component partNumber="511-500922" refDes="C524" circuitNumber="1" x="-264.062" y="145.056" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C524" circuitNumber="2" x="-127.922" y="145.056" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C524" circuitNumber="3" x="-264.062" y="40.256" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C524" circuitNumber="4" x="-127.922" y="40.256" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C525" circuitNumber="1" x="-267.110" y="145.056" z="0.000" rz="180.00"/>
		<Component partNumber="511-500922" refDes="C525" circuitNumber="2" x="-130.970" y="145.056" z="0.000" rz="180.00"/>
		<Component partNumber="511-500922" refDes="C525" circuitNumber="3" x="-267.110" y="40.256" z="0.000" rz="180.00"/>
		<Component partNumber="511-500922" refDes="C525" circuitNumber="4" x="-130.970" y="40.256" z="0.000" rz="180.00"/>
		<Component partNumber="511-500922" refDes="C533" circuitNumber="1" x="-264.062" y="147.108" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C533" circuitNumber="2" x="-127.922" y="147.108" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C533" circuitNumber="3" x="-264.062" y="42.308" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C533" circuitNumber="4" x="-127.922" y="42.308" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C534" circuitNumber="1" x="-267.110" y="147.108" z="0.000" rz="180.00"/>
		<Component partNumber="511-500922" refDes="C534" circuitNumber="2" x="-130.970" y="147.108" z="0.000" rz="180.00"/>
		<Component partNumber="511-500922" refDes="C534" circuitNumber="3" x="-267.110" y="42.308" z="0.000" rz="180.00"/>
		<Component partNumber="511-500922" refDes="C534" circuitNumber="4" x="-130.970" y="42.308" z="0.000" rz="180.00"/>
		<Component partNumber="511-500922" refDes="C612" circuitNumber="1" x="-152.913" y="186.303" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C612" circuitNumber="2" x="-16.773" y="186.303" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C612" circuitNumber="3" x="-152.913" y="81.503" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C612" circuitNumber="4" x="-16.773" y="81.503" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C619" circuitNumber="1" x="-228.375" y="189.786" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C619" circuitNumber="2" x="-92.235" y="189.786" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C619" circuitNumber="3" x="-228.375" y="84.986" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C619" circuitNumber="4" x="-92.235" y="84.986" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C622" circuitNumber="1" x="-228.375" y="191.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C622" circuitNumber="2" x="-92.235" y="191.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C622" circuitNumber="3" x="-228.375" y="87.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C622" circuitNumber="4" x="-92.235" y="87.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C627" circuitNumber="1" x="-224.565" y="193.161" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C627" circuitNumber="2" x="-88.425" y="193.161" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C627" circuitNumber="3" x="-224.565" y="88.361" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C627" circuitNumber="4" x="-88.425" y="88.361" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C649" circuitNumber="1" x="-242.630" y="197.098" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C649" circuitNumber="2" x="-106.490" y="197.098" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C649" circuitNumber="3" x="-242.630" y="92.298" z="0.000" rz="270.00"/>
		<Component partNumber="511-500922" refDes="C649" circuitNumber="4" x="-106.490" y="92.298" z="0.000" rz="270.00"/>
		<Component partNumber="512-500440" refDes="L7" circuitNumber="1" x="-227.486" y="203.743" z="0.000" rz="0.00"/>
		<Component partNumber="512-500440" refDes="L7" circuitNumber="2" x="-91.346" y="203.743" z="0.000" rz="0.00"/>
		<Component partNumber="512-500440" refDes="L7" circuitNumber="3" x="-227.486" y="98.943" z="0.000" rz="0.00"/>
		<Component partNumber="512-500440" refDes="L7" circuitNumber="4" x="-91.346" y="98.943" z="0.000" rz="0.00"/>
		<Component partNumber="512-500441" refDes="FB1" circuitNumber="1" x="-260.348" y="123.411" z="0.000" rz="270.00"/>
		<Component partNumber="512-500441" refDes="FB1" circuitNumber="2" x="-124.208" y="123.411" z="0.000" rz="270.00"/>
		<Component partNumber="512-500441" refDes="FB1" circuitNumber="3" x="-260.348" y="18.611" z="0.000" rz="270.00"/>
		<Component partNumber="512-500441" refDes="FB1" circuitNumber="4" x="-124.208" y="18.611" z="0.000" rz="270.00"/>
		<Component partNumber="512-500446" refDes="L8" circuitNumber="1" x="-220.638" y="205.392" z="0.000" rz="90.00"/>
		<Component partNumber="512-500446" refDes="L8" circuitNumber="2" x="-84.498" y="205.392" z="0.000" rz="90.00"/>
		<Component partNumber="512-500446" refDes="L8" circuitNumber="3" x="-220.638" y="100.592" z="0.000" rz="90.00"/>
		<Component partNumber="512-500446" refDes="L8" circuitNumber="4" x="-84.498" y="100.592" z="0.000" rz="90.00"/>
		<Component partNumber="512-500447" refDes="L2" circuitNumber="1" x="-233.247" y="193.034" z="0.000" rz="0.00"/>
		<Component partNumber="512-500447" refDes="L2" circuitNumber="2" x="-97.107" y="193.034" z="0.000" rz="0.00"/>
		<Component partNumber="512-500447" refDes="L2" circuitNumber="3" x="-233.247" y="88.234" z="0.000" rz="0.00"/>
		<Component partNumber="512-500447" refDes="L2" circuitNumber="4" x="-97.107" y="88.234" z="0.000" rz="0.00"/>
		<Component partNumber="512-500447" refDes="L3" circuitNumber="1" x="-238.535" y="193.034" z="0.000" rz="180.00"/>
		<Component partNumber="512-500447" refDes="L3" circuitNumber="2" x="-102.395" y="193.034" z="0.000" rz="180.00"/>
		<Component partNumber="512-500447" refDes="L3" circuitNumber="3" x="-238.535" y="88.234" z="0.000" rz="180.00"/>
		<Component partNumber="512-500447" refDes="L3" circuitNumber="4" x="-102.395" y="88.234" z="0.000" rz="180.00"/>
		<Component partNumber="512-500448" refDes="L4" circuitNumber="1" x="-227.486" y="194.558" z="0.000" rz="0.00"/>
		<Component partNumber="512-500448" refDes="L4" circuitNumber="2" x="-91.346" y="194.558" z="0.000" rz="0.00"/>
		<Component partNumber="512-500448" refDes="L4" circuitNumber="3" x="-227.486" y="89.758" z="0.000" rz="0.00"/>
		<Component partNumber="512-500448" refDes="L4" circuitNumber="4" x="-91.346" y="89.758" z="0.000" rz="0.00"/>
		<Component partNumber="512-500448" refDes="L6" circuitNumber="1" x="-227.486" y="197.958" z="0.000" rz="0.00"/>
		<Component partNumber="512-500448" refDes="L6" circuitNumber="2" x="-91.346" y="197.958" z="0.000" rz="0.00"/>
		<Component partNumber="512-500448" refDes="L6" circuitNumber="3" x="-227.486" y="93.158" z="0.000" rz="0.00"/>
		<Component partNumber="512-500448" refDes="L6" circuitNumber="4" x="-91.346" y="93.158" z="0.000" rz="0.00"/>
		<Component partNumber="512-500450" refDes="L1" circuitNumber="1" x="-263.713" y="159.767" z="0.000" rz="0.00"/>
		<Component partNumber="512-500450" refDes="L1" circuitNumber="2" x="-127.573" y="159.767" z="0.000" rz="0.00"/>
		<Component partNumber="512-500450" refDes="L1" circuitNumber="3" x="-263.713" y="54.967" z="0.000" rz="0.00"/>
		<Component partNumber="512-500450" refDes="L1" circuitNumber="4" x="-127.573" y="54.967" z="0.000" rz="0.00"/>
		<Component partNumber="512-500450" refDes="L5" circuitNumber="1" x="-251.111" y="195.447" z="0.000" rz="270.00"/>
		<Component partNumber="512-500450" refDes="L5" circuitNumber="2" x="-114.971" y="195.447" z="0.000" rz="270.00"/>
		<Component partNumber="512-500450" refDes="L5" circuitNumber="3" x="-251.111" y="90.647" z="0.000" rz="270.00"/>
		<Component partNumber="512-500450" refDes="L5" circuitNumber="4" x="-114.971" y="90.647" z="0.000" rz="270.00"/>
		<Component partNumber="512-500451" refDes="L9" circuitNumber="1" x="-251.361" y="205.303" z="0.000" rz="90.00"/>
		<Component partNumber="512-500451" refDes="L9" circuitNumber="2" x="-115.221" y="205.303" z="0.000" rz="90.00"/>
		<Component partNumber="512-500451" refDes="L9" circuitNumber="3" x="-251.361" y="100.503" z="0.000" rz="90.00"/>
		<Component partNumber="512-500451" refDes="L9" circuitNumber="4" x="-115.221" y="100.503" z="0.000" rz="90.00"/>
		<Component partNumber="513-500518" refDes="Q22" circuitNumber="1" x="-245.860" y="201.924" z="0.000" rz="90.00"/>
		<Component partNumber="513-500518" refDes="Q22" circuitNumber="2" x="-109.720" y="201.924" z="0.000" rz="90.00"/>
		<Component partNumber="513-500518" refDes="Q22" circuitNumber="3" x="-245.860" y="97.124" z="0.000" rz="90.00"/>
		<Component partNumber="513-500518" refDes="Q22" circuitNumber="4" x="-109.720" y="97.124" z="0.000" rz="90.00"/>
		<Component partNumber="513-500518" refDes="Q23" circuitNumber="1" x="-245.860" y="204.591" z="0.000" rz="90.00"/>
		<Component partNumber="513-500518" refDes="Q23" circuitNumber="2" x="-109.720" y="204.591" z="0.000" rz="90.00"/>
		<Component partNumber="513-500518" refDes="Q23" circuitNumber="3" x="-245.860" y="99.791" z="0.000" rz="90.00"/>
		<Component partNumber="513-500518" refDes="Q23" circuitNumber="4" x="-109.720" y="99.791" z="0.000" rz="90.00"/>
		<Component partNumber="513-500519" refDes="D14" circuitNumber="1" x="-268.038" y="141.809" z="0.000" rz="90.00"/>
		<Component partNumber="513-500519" refDes="D14" circuitNumber="2" x="-131.898" y="141.809" z="0.000" rz="90.00"/>
		<Component partNumber="513-500519" refDes="D14" circuitNumber="3" x="-268.038" y="37.009" z="0.000" rz="90.00"/>
		<Component partNumber="513-500519" refDes="D14" circuitNumber="4" x="-131.898" y="37.009" z="0.000" rz="90.00"/>
		<Component partNumber="513-500519" refDes="D16" circuitNumber="1" x="-257.138" y="163.059" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D16" circuitNumber="2" x="-120.998" y="163.059" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D16" circuitNumber="3" x="-257.138" y="58.259" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D16" circuitNumber="4" x="-120.998" y="58.259" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D17" circuitNumber="1" x="-228.787" y="187.319" z="0.000" rz="180.00"/>
		<Component partNumber="513-500519" refDes="D17" circuitNumber="2" x="-92.647" y="187.319" z="0.000" rz="180.00"/>
		<Component partNumber="513-500519" refDes="D17" circuitNumber="3" x="-228.787" y="82.519" z="0.000" rz="180.00"/>
		<Component partNumber="513-500519" refDes="D17" circuitNumber="4" x="-92.647" y="82.519" z="0.000" rz="180.00"/>
		<Component partNumber="513-500519" refDes="D19" circuitNumber="1" x="-263.681" y="137.986" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D19" circuitNumber="2" x="-127.541" y="137.986" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D19" circuitNumber="3" x="-263.681" y="33.186" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D19" circuitNumber="4" x="-127.541" y="33.186" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D20" circuitNumber="1" x="-255.688" y="163.059" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D20" circuitNumber="2" x="-119.548" y="163.059" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D20" circuitNumber="3" x="-255.688" y="58.259" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D20" circuitNumber="4" x="-119.548" y="58.259" z="0.000" rz="0.00"/>
		<Component partNumber="513-500520" refDes="Q17" circuitNumber="1" x="-241.361" y="187.088" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q17" circuitNumber="2" x="-105.221" y="187.088" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q17" circuitNumber="3" x="-241.361" y="82.288" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q17" circuitNumber="4" x="-105.221" y="82.288" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q19" circuitNumber="1" x="-225.188" y="189.732" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q19" circuitNumber="2" x="-89.048" y="189.732" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q19" circuitNumber="3" x="-225.188" y="84.932" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q19" circuitNumber="4" x="-89.048" y="84.932" z="0.000" rz="270.00"/>
		<Component partNumber="513-500522" refDes="D13" circuitNumber="1" x="-213.900" y="127.499" z="0.000" rz="0.00"/>
		<Component partNumber="513-500522" refDes="D13" circuitNumber="2" x="-77.760" y="127.499" z="0.000" rz="0.00"/>
		<Component partNumber="513-500522" refDes="D13" circuitNumber="3" x="-213.900" y="22.699" z="0.000" rz="0.00"/>
		<Component partNumber="513-500522" refDes="D13" circuitNumber="4" x="-77.760" y="22.699" z="0.000" rz="0.00"/>
		<Component partNumber="513-500524" refDes="Q20" circuitNumber="1" x="-245.636" y="195.320" z="0.000" rz="270.00"/>
		<Component partNumber="513-500524" refDes="Q20" circuitNumber="2" x="-109.496" y="195.320" z="0.000" rz="270.00"/>
		<Component partNumber="513-500524" refDes="Q20" circuitNumber="3" x="-245.636" y="90.520" z="0.000" rz="270.00"/>
		<Component partNumber="513-500524" refDes="Q20" circuitNumber="4" x="-109.496" y="90.520" z="0.000" rz="270.00"/>
		<Component partNumber="513-500524" refDes="Q21" circuitNumber="1" x="-245.663" y="198.213" z="0.000" rz="180.00"/>
		<Component partNumber="513-500524" refDes="Q21" circuitNumber="2" x="-109.523" y="198.213" z="0.000" rz="180.00"/>
		<Component partNumber="513-500524" refDes="Q21" circuitNumber="3" x="-245.663" y="93.413" z="0.000" rz="180.00"/>
		<Component partNumber="513-500524" refDes="Q21" circuitNumber="4" x="-109.523" y="93.413" z="0.000" rz="180.00"/>
		<Component partNumber="514-500139" refDes="Y1" circuitNumber="1" x="-259.666" y="119.374" z="0.000" rz="270.00"/>
		<Component partNumber="514-500139" refDes="Y1" circuitNumber="2" x="-123.526" y="119.374" z="0.000" rz="270.00"/>
		<Component partNumber="514-500139" refDes="Y1" circuitNumber="3" x="-259.666" y="14.574" z="0.000" rz="270.00"/>
		<Component partNumber="514-500139" refDes="Y1" circuitNumber="4" x="-123.526" y="14.574" z="0.000" rz="270.00"/>
		<Component partNumber="515-500767" refDes="VR44" circuitNumber="1" x="-265.078" y="152.061" z="0.000" rz="270.00"/>
		<Component partNumber="515-500767" refDes="VR44" circuitNumber="2" x="-128.938" y="152.061" z="0.000" rz="270.00"/>
		<Component partNumber="515-500767" refDes="VR44" circuitNumber="3" x="-265.078" y="47.261" z="0.000" rz="270.00"/>
		<Component partNumber="515-500767" refDes="VR44" circuitNumber="4" x="-128.938" y="47.261" z="0.000" rz="270.00"/>
		<Component partNumber="515-500771" refDes="VR56" circuitNumber="1" x="-220.120" y="199.384" z="0.000" rz="180.00"/>
		<Component partNumber="515-500771" refDes="VR56" circuitNumber="2" x="-83.980" y="199.384" z="0.000" rz="180.00"/>
		<Component partNumber="515-500771" refDes="VR56" circuitNumber="3" x="-220.120" y="94.584" z="0.000" rz="180.00"/>
		<Component partNumber="515-500771" refDes="VR56" circuitNumber="4" x="-83.980" y="94.584" z="0.000" rz="180.00"/>
		<Component partNumber="520-M5" refDes="U24" circuitNumber="1" x="-233.836" y="176.016" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U24" circuitNumber="2" x="-97.696" y="176.016" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U24" circuitNumber="3" x="-233.836" y="71.216" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U24" circuitNumber="4" x="-97.696" y="71.216" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U34" circuitNumber="1" x="-163.327" y="124.327" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U34" circuitNumber="2" x="-27.187" y="124.327" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U34" circuitNumber="3" x="-163.327" y="19.527" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U34" circuitNumber="4" x="-27.187" y="19.527" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U35" circuitNumber="1" x="-182.377" y="124.327" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U35" circuitNumber="2" x="-46.237" y="124.327" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U35" circuitNumber="3" x="-182.377" y="19.527" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U35" circuitNumber="4" x="-46.237" y="19.527" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U38" circuitNumber="1" x="-163.327" y="139.821" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U38" circuitNumber="2" x="-27.187" y="139.821" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U38" circuitNumber="3" x="-163.327" y="35.021" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U38" circuitNumber="4" x="-27.187" y="35.021" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U39" circuitNumber="1" x="-182.377" y="139.821" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U39" circuitNumber="2" x="-46.237" y="139.821" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U39" circuitNumber="3" x="-182.377" y="35.021" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U39" circuitNumber="4" x="-46.237" y="35.021" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U40" circuitNumber="1" x="-145.293" y="140.730" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U40" circuitNumber="2" x="-9.153" y="140.730" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U40" circuitNumber="3" x="-145.293" y="35.930" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U40" circuitNumber="4" x="-9.153" y="35.930" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U45" circuitNumber="1" x="-163.327" y="155.315" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U45" circuitNumber="2" x="-27.187" y="155.315" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U45" circuitNumber="3" x="-163.327" y="50.515" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U45" circuitNumber="4" x="-27.187" y="50.515" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U46" circuitNumber="1" x="-182.377" y="155.315" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U46" circuitNumber="2" x="-46.237" y="155.315" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U46" circuitNumber="3" x="-182.377" y="50.515" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U46" circuitNumber="4" x="-46.237" y="50.515" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U47" circuitNumber="1" x="-145.293" y="161.030" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U47" circuitNumber="2" x="-9.153" y="161.030" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U47" circuitNumber="3" x="-145.293" y="56.230" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U47" circuitNumber="4" x="-9.153" y="56.230" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U49" circuitNumber="1" x="-163.327" y="170.809" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U49" circuitNumber="2" x="-27.187" y="170.809" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U49" circuitNumber="3" x="-163.327" y="66.009" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U49" circuitNumber="4" x="-27.187" y="66.009" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U50" circuitNumber="1" x="-182.377" y="170.809" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U50" circuitNumber="2" x="-46.237" y="170.809" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U50" circuitNumber="3" x="-182.377" y="66.009" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U50" circuitNumber="4" x="-46.237" y="66.009" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U51" circuitNumber="1" x="-218.342" y="176.016" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U51" circuitNumber="2" x="-82.202" y="176.016" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U51" circuitNumber="3" x="-218.342" y="71.216" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U51" circuitNumber="4" x="-82.202" y="71.216" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U54" circuitNumber="1" x="-163.327" y="186.303" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U54" circuitNumber="2" x="-27.187" y="186.303" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U54" circuitNumber="3" x="-163.327" y="81.503" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U54" circuitNumber="4" x="-27.187" y="81.503" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U55" circuitNumber="1" x="-182.377" y="186.303" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U55" circuitNumber="2" x="-46.237" y="186.303" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U55" circuitNumber="3" x="-182.377" y="81.503" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U55" circuitNumber="4" x="-46.237" y="81.503" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U58" circuitNumber="1" x="-163.327" y="201.797" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U58" circuitNumber="2" x="-27.187" y="201.797" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U58" circuitNumber="3" x="-163.327" y="96.997" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U58" circuitNumber="4" x="-27.187" y="96.997" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U59" circuitNumber="1" x="-182.377" y="201.797" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U59" circuitNumber="2" x="-46.237" y="201.797" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U59" circuitNumber="3" x="-182.377" y="96.997" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U59" circuitNumber="4" x="-46.237" y="96.997" z="0.000" rz="0.00"/>
		<Component partNumber="520-NEA" refDes="U36" circuitNumber="1" x="-252.415" y="131.842" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U36" circuitNumber="2" x="-116.275" y="131.842" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U36" circuitNumber="3" x="-252.415" y="27.042" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U36" circuitNumber="4" x="-116.275" y="27.042" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U41" circuitNumber="1" x="-252.415" y="143.339" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U41" circuitNumber="2" x="-116.275" y="143.339" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U41" circuitNumber="3" x="-252.415" y="38.539" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U41" circuitNumber="4" x="-116.275" y="38.539" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U43" circuitNumber="1" x="-252.415" y="154.836" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U43" circuitNumber="2" x="-116.275" y="154.836" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U43" circuitNumber="3" x="-252.415" y="50.036" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U43" circuitNumber="4" x="-116.275" y="50.036" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U48" circuitNumber="1" x="-247.791" y="166.333" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U48" circuitNumber="2" x="-111.651" y="166.333" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U48" circuitNumber="3" x="-247.791" y="61.533" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U48" circuitNumber="4" x="-111.651" y="61.533" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U53" circuitNumber="1" x="-247.791" y="177.830" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U53" circuitNumber="2" x="-111.651" y="177.830" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U53" circuitNumber="3" x="-247.791" y="73.030" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U53" circuitNumber="4" x="-111.651" y="73.030" z="0.000" rz="180.00"/>
		<Component partNumber="521-501370" refDes="VR57" circuitNumber="1" x="-235.487" y="200.273" z="0.000" rz="180.00"/>
		<Component partNumber="521-501370" refDes="VR57" circuitNumber="2" x="-99.347" y="200.273" z="0.000" rz="180.00"/>
		<Component partNumber="521-501370" refDes="VR57" circuitNumber="3" x="-235.487" y="95.473" z="0.000" rz="180.00"/>
		<Component partNumber="521-501370" refDes="VR57" circuitNumber="4" x="-99.347" y="95.473" z="0.000" rz="180.00"/>
		<Component partNumber="521-501382" refDes="U37" circuitNumber="1" x="-263.935" y="133.518" z="0.000" rz="90.00"/>
		<Component partNumber="521-501382" refDes="U37" circuitNumber="2" x="-127.795" y="133.518" z="0.000" rz="90.00"/>
		<Component partNumber="521-501382" refDes="U37" circuitNumber="3" x="-263.935" y="28.718" z="0.000" rz="90.00"/>
		<Component partNumber="521-501382" refDes="U37" circuitNumber="4" x="-127.795" y="28.718" z="0.000" rz="90.00"/>
		<Circuit number="1" type="3440CB-PD9-M5(C)" sizeX="133.600" sizeY="95.800" x="-269.738" y="114.959" z="0.000" rz="0.00"/>
		<Circuit number="2" type="3440CB-PD9-M5(C)" sizeX="133.600" sizeY="95.800" x="-133.598" y="114.959" z="0.000" rz="0.00"/>
		<Circuit number="3" type="3440CB-PD9-M5(C)" sizeX="133.600" sizeY="95.800" x="-269.738" y="10.159" z="0.000" rz="0.00"/>
		<Circuit number="4" type="3440CB-PD9-M5(C)" sizeX="133.600" sizeY="95.800" x="-133.598" y="10.159" z="0.000" rz="0.00"/>
		<Model id="r1574.8">
			<Model xmlns="" xmlns:ns2="http://api.assembleon.com/pp/v2">
				<General>
					<ModelId>r1574.8</ModelId>
					<Creator>Assembleon</Creator>
					<SyntaxVersion>2.1</SyntaxVersion>
					<Release/>
					<LastModified>2007-04-02T00:00:00Z</LastModified>
				</General>
				<ModelDescription>
					<ModelType>Badmark</ModelType>
					<ModelOrientation>TopView</ModelOrientation>
					<Uncertainty>
						<X>1.3</X>
						<Y>1.3</Y>
					</Uncertainty>
					<Camera>
						<CameraId>BA</CameraId>
						<LightLevels>
							<BF>70</BF>
							<MF>0</MF>
							<DF>40</DF>
							<BL>0</BL>
						</LightLevels>
					</Camera>
					<Feature>
						<Size>
							<X>0.2</X>
							<Y>0.2</Y>
						</Size>
						<Polarity>Black</Polarity>
						<Badmark>
							<GreyValueThreshold>75</GreyValueThreshold>
							<PercentageOfROI>50</PercentageOfROI>
						</Badmark>
					</Feature>
				</ModelDescription>
			</Model>
		</Model>
		<Model id="r1016">
			<Model xmlns="" xmlns:ns2="http://api.assembleon.com/pp/v2">
				<General>
					<ModelId>r1016</ModelId>
					<Creator>Saved</Creator>
					<SyntaxVersion>2.1</SyntaxVersion>
					<LastModified>2018-01-02T16:10:44Z</LastModified>
				</General>
				<ModelDescription>
					<ModelType>Fiducial</ModelType>
					<ModelOrientation>TopView</ModelOrientation>
					<Camera>
						<CameraId>BA</CameraId>
						<LightLevels>
							<BF>38</BF>
							<DF>49</DF>
						</LightLevels>
					</Camera>
					<Feature>
						<Position>
							<X>0.000</X>
							<Y>0.000</Y>
						</Position>
						<Size>
							<X>1.000</X>
							<Y>1.000</Y>
						</Size>
						<Polarity>White</Polarity>
						<Ellipse/>
					</Feature>
				</ModelDescription>
			</Model>
		</Model>
	</Board>
	<Setup>
		<Transport direction="LEFT_TO_RIGHT" stopperInX="-147.580">
			<IndexStep size="68.685" returnStroke="-274.737"/>
			<IndexStep size="68.684"/>
			<IndexStep size="68.684"/>
			<IndexStep size="68.684"/>
		</Transport>
		<Section number="1">
			<Trolley type="A_SERIES_FDR_TROLLEY">
				<Feeder number="1" type="ITF2_08">
					<Lane number="1" partNumber="510-502303" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="2" type="ITF2_08">
					<Lane number="1" partNumber="510-501547" shapeId="SMR0402"/>
				</Feeder>
				<Feeder number="3" type="ITF2_08">
					<Lane number="1" partNumber="511-500615" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="4" type="ITF2_08">
					<Lane number="1" partNumber="511-500590" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="5" type="ITF2_08">
					<Lane number="1" partNumber="511-500646" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="9" type="ITF2_08">
					<Lane number="1" partNumber="511-500919" shapeId="SMC0402"/>
				</Feeder>
				<Feeder number="10" type="ITF2_08">
					<Lane number="1" partNumber="511-500589" shapeId="SMC0402_1.0X0.6"/>
				</Feeder>
				<Feeder number="11" type="ITF2_08">
					<Lane number="1" partNumber="511-500916" shapeId="SMC0603"/>
				</Feeder>
				<Feeder number="12" type="ITF2_08">
					<Lane number="1" partNumber="515-500773" shapeId="SON6_150X105P050H060"/>
				</Feeder>
				<Feeder number="13" type="ITF2_08">
					<Lane number="1" partNumber="515-500764" shapeId="SMF0603_H055"/>
				</Feeder>
				<Feeder number="15" type="ITF2_08">
					<Lane number="1" partNumber="510-502325" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="16" type="ITF2_08">
					<Lane number="1" partNumber="510-501070" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="17" type="ITF2_08">
					<Lane number="1" partNumber="510-500956A" shapeId="SMR0402"/>
				</Feeder>
				<Feeder number="18" type="ITF2_08">
					<Lane number="1" partNumber="511-500590" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="19" type="ITF2_08">
					<Lane number="1" partNumber="510-500955" shapeId="SMR0402"/>
				</Feeder>
				<Feeder number="23" type="ITF2_08">
					<Lane number="1" partNumber="510-501108" shapeId="SMR0402"/>
				</Feeder>
				<Feeder number="24" type="ITF2_08">
					<Lane number="1" partNumber="511-500615" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="25" type="ITF2_08">
					<Lane number="1" partNumber="511-500590" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="26" type="ITF2_08">
					<Lane number="1" partNumber="511-500556" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="27" type="ITF2_08">
					<Lane number="1" partNumber="511-500253" shapeId="SMC0201"/>
				</Feeder>
			</Trolley>
			<Robot number="1" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="2" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL3"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="3" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="4" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
		</Section>
		<Section number="2">
			<Trolley type="A_SERIES_FDR_TROLLEY">
				<Feeder number="1" type="ITF2_08">
					<Lane number="1" partNumber="511-500831" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="2" type="ITF2_08">
					<Lane number="1" partNumber="511-500892" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="3" type="ITF2_08">
					<Lane number="1" partNumber="510-501070" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="4" type="ITF2_08">
					<Lane number="1" partNumber="511-500590" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="5" type="ITF2_08">
					<Lane number="1" partNumber="511-500616" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="9" type="ITF2_08">
					<Lane number="1" partNumber="511-500919" shapeId="SMC0402"/>
				</Feeder>
				<Feeder number="10" type="ITF2_08">
					<Lane number="1" partNumber="511-500589" shapeId="SMC0402_1.0X0.6"/>
				</Feeder>
				<Feeder number="11" type="ITF2_08">
					<Lane number="1" partNumber="511-500572" shapeId="SMC0402_1.0X0.6"/>
				</Feeder>
				<Feeder number="12" type="ITF2_08">
					<Lane number="1" partNumber="511-500557" shapeId="SMC0402"/>
				</Feeder>
				<Feeder number="13" type="ITF2_08">
					<Lane number="1" partNumber="511-500643" shapeId="SMC0402"/>
				</Feeder>
				<Feeder number="15" type="ITF2_08">
					<Lane number="1" partNumber="511-500893" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="16" type="ITF2_08">
					<Lane number="1" partNumber="511-500896" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="17" type="ITF2_08">
					<Lane number="1" partNumber="511-500615" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="18" type="ITF2_08">
					<Lane number="1" partNumber="511-500590" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="19" type="ITF2_08">
					<Lane number="1" partNumber="510-500998" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="23" type="ITF2_08">
					<Lane number="1" partNumber="511-500866" shapeId="SMC0603"/>
				</Feeder>
				<Feeder number="24" type="ITF2_08">
					<Lane number="1" partNumber="510-501843" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="25" type="ITF2_08">
					<Lane number="1" partNumber="511-500615" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="27" type="ITF2_12" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="521-501382" shapeId="SON8_6_1X5_1P127H080"/>
				</Feeder>
			</Trolley>
			<Robot number="5" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="6" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL3"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="7" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="8" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
					<Toolbit type="CPL3"/>
					<Toolbit type="L5"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
		</Section>
		<Section number="3">
			<Trolley type="A_SERIES_FDR_TROLLEY">
				<Feeder number="1" type="ITF2_08">
					<Lane number="1" partNumber="510-502297" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="2" type="ITF2_08">
					<Lane number="1" partNumber="510-500866" shapeId="SMR0402"/>
				</Feeder>
				<Feeder number="3" type="ITF2_08">
					<Lane number="1" partNumber="511-500615" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="4" type="ITF2_08">
					<Lane number="1" partNumber="511-500590" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="5" type="ITF2_08">
					<Lane number="1" partNumber="513-500517" shapeId="SOD923_65X85H040"/>
				</Feeder>
				<Feeder number="9" type="ITF2_08">
					<Lane number="1" partNumber="511-500919" shapeId="SMC0402"/>
				</Feeder>
				<Feeder number="10" type="ITF2_08">
					<Lane number="1" partNumber="511-500572" shapeId="SMC0402_1.0X0.6"/>
				</Feeder>
				<Feeder number="11" type="ITF2_08">
					<Lane number="1" partNumber="511-500589" shapeId="SMC0402_1.0X0.6"/>
				</Feeder>
				<Feeder number="12" type="ITF2_08">
					<Lane number="1" partNumber="512-500441" shapeId="SMFB0402"/>
				</Feeder>
				<Feeder number="13" type="ITF2_08">
					<Lane number="1" partNumber="511-500895" shapeId="SMC0402"/>
				</Feeder>
				<Feeder number="15" type="ITF2_08">
					<Lane number="1" partNumber="511-500918" shapeId="SMC0805"/>
				</Feeder>
				<Feeder number="16" type="ITF2_08">
					<Lane number="1" partNumber="511-500920" shapeId="SMC1206"/>
				</Feeder>
				<Feeder number="17" type="ITF2_08">
					<Lane number="1" partNumber="513-500520" shapeId="SOT6_125X170P50H060"/>
				</Feeder>
				<Feeder number="18" type="ITF2_08">
					<Lane number="1" partNumber="511-500922" shapeId="SMC0805"/>
				</Feeder>
				<Feeder number="19" type="ITF2_08">
					<Lane number="1" partNumber="513-500522" shapeId="SOD123W_1900X2900H110"/>
				</Feeder>
				<Feeder number="23" type="ITF2_08">
					<Lane number="1" partNumber="510-500881" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="24" type="ITF2_08">
					<Lane number="1" partNumber="511-500595" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="25" type="ITF2_08">
					<Lane number="1" partNumber="511-500590" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="27" type="ITF2_16" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="512-500451" shapeId="SMI7300X7100H2000_NRKO"/>
				</Feeder>
			</Trolley>
			<Robot number="9" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="10" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL3"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="11" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL4"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="12" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
					<Toolbit type="L7"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
		</Section>
		<Section number="4">
			<Trolley type="A_SERIES_FDR_TROLLEY">
				<Feeder number="1" type="ITF2_08">
					<Lane number="1" partNumber="511-500615" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="2" type="ITF2_24" topGuidePosition="LEFT">
					<Lane number="1" partNumber="520-NEA" shapeId="FBGA78_DDR4_11X7"/>
				</Feeder>
				<Feeder number="5" type="ITF2_24" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="520-SA" shapeId="FBGA78_DDR4_11X7"/>
				</Feeder>
				<Feeder number="9" type="ITF2_08">
					<Lane number="1" partNumber="511-500589" shapeId="SMC0402_1.0X0.6"/>
				</Feeder>
				<Feeder number="11" type="ITF2_32" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="520-M5" shapeId="BGA132_12X18P100H150"/>
				</Feeder>
				<Feeder number="15" type="ITF2_08">
					<Lane number="1" partNumber="510-502324" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="16" type="ITF2_08">
					<Lane number="1" partNumber="510-501765" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="18" type="ITF2_32" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="520-M5" shapeId="BGA132_12X18P100H150"/>
				</Feeder>
				<Feeder number="23" type="ITF2_08">
					<Lane number="1" partNumber="510-501070" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="24" type="ITF2_08">
					<Lane number="1" partNumber="510-501765" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="26" type="ITF2_32" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="520-M5" shapeId="BGA132_12X18P100H150"/>
				</Feeder>
			</Trolley>
			<Robot number="13" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="V7"/>
					<Toolbit type="CPL2"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="14" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL3"/>
					<Toolbit type="V7"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="15" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
					<Toolbit type="V7"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="16" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="V7"/>
					<Toolbit type="CPL2"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
		</Section>
		<Section number="5">
			<Trolley type="A_SERIES_FDR_TROLLEY">
				<Feeder number="1" type="ITF2_08">
					<Lane number="1" partNumber="511-500615" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="3" type="ITF2_24" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="520-NEA" shapeId="FBGA78_DDR4_11X7"/>
				</Feeder>
				<Feeder number="5" type="ITF2_24" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="520-SA" shapeId="FBGA78_DDR4_11X7"/>
				</Feeder>
				<Feeder number="9" type="ITF2_32" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="520-M5" shapeId="BGA132_12X18P100H150"/>
				</Feeder>
				<Feeder number="12" type="ITF2_08">
					<Lane number="1" partNumber="511-500615" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="13" type="ITF2_08">
					<Lane number="1" partNumber="510-500980" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="15" type="ITF2_08">
					<Lane number="1" partNumber="510-501177" shapeId="SMR0201"/>
				</Feeder>
				<Feeder number="16" type="ITF2_08">
					<Lane number="1" partNumber="513-500524" shapeId="DFN6_210SQR_P065H080"/>
				</Feeder>
				<Feeder number="17" type="ITF2_08">
					<Lane number="1" partNumber="511-500615" shapeId="SMC0201"/>
				</Feeder>
				<Feeder number="19" type="ITF2_16" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="521-501370" shapeId="QFN68_810X810P040H080"/>
				</Feeder>
				<Feeder number="20" type="ITF2_08">
					<Lane number="1" partNumber="510-500951" shapeId="SMR0402"/>
				</Feeder>
				<Feeder number="21" type="ITF2_08">
					<Lane number="1" partNumber="521-501375" shapeId="BGA4_079X079H625"/>
				</Feeder>
				<Feeder number="24" type="ITF2_16" topGuidePosition="RIGHT">
					<Lane number="1" partNumber="531-500464" shapeId="HDMI_MICRO_TYPE_D_RECPT_464"/>
				</Feeder>
				<Feeder number="26" type="ITF2_16" topGuidePosition="LEFT">
					<Lane number="1" partNumber="541-500133" shapeId="PEM_SPACER_4MM"/>
				</Feeder>
			</Trolley>
			<Robot number="17" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="V7"/>
					<Toolbit type="CPL2"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="18" type="CPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
					<Toolbit type="V7"/>
				</TEU>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
			<Robot number="19" type="SPR" maxComponentHeightProducingSide="10.000">
				<TEU type="FCMIII_rel1.0">
					<Toolbit type="CPL2"/>
					<Toolbit type="L7"/>
					<Toolbit type="V3"/>
					<Toolbit type="SL619"/>
					<Toolbit type="CPL4"/>
				</TEU>
				<AlignmentModule id="CV_LFOV" type="BOTTOM_VIEW" position="CV1"/>
				<AlignmentModule id="NCLA" type="SIDE_VIEW"/>
			</Robot>
		</Section>
	</Setup>
	<Actions robotNumber="1">
		<Index minBoardNumber="2" maxBoardNumber="2">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C531" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="0_1_0_F2_5_3" boardNumber="2" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="0_1_0_F0_2_1" boardNumber="2" refDes="F0_2" circuitNumber="1"/>
			<ReadFiducial refId="0_1_0_F3_1_4" boardNumber="2" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C491" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C508" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C623" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C586" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="R73" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C483" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="R73" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C552" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<ReadFiducial refId="0_1_1_F2_1_3" boardNumber="2" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="0_1_1_F0_6_1" boardNumber="2" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="0_1_1_F2_5_3" boardNumber="2" refDes="F2_5" circuitNumber="3"/>
			<ReadBadmark boardNumber="2" alignWith="AM_5" refDes="B2" circuitNumber="3"/>
			<ReadBadmark boardNumber="2" alignWith="AM_5" refDes="B0" circuitNumber="1"/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C592" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C500" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C472" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C644" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C501" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C565" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C580" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C636" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C474" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C497" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C515" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C580" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C643" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="1" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_8" refDes="C510" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="0_1_2_F2_4_3" boardNumber="2" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="0_1_2_F2_1_3" boardNumber="2" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="0_1_2_F0_0_1" boardNumber="2" refDes="F0_0" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C528" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C587" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C586" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C478" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="R73" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C491" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="R73" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="1" maxBoardNumber="1">
			<ReadFiducial refId="0_0_3_F3_1_4" boardNumber="1" refDes="F3_1" circuitNumber="4"/>
			<ReadFiducial refId="0_0_3_F1_6_2" boardNumber="1" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="0_0_3_F3_5_4" boardNumber="1" refDes="F3_5" circuitNumber="4"/>
			<ReadBadmark boardNumber="1" alignWith="AM_1" refDes="B3" circuitNumber="4"/>
			<ReadBadmark boardNumber="1" alignWith="AM_1" refDes="B1" circuitNumber="2"/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C640" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C561" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C515" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C642" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C592" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C685" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C476" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C501" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C580" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C592" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C606" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C685" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="2">
		<Index minBoardNumber="2" maxBoardNumber="3">
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="3" alignWith="AM_14" refDes="C547" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_2_0_F2_4_3" boardNumber="3" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="1_2_0_F0_0_1" boardNumber="3" refDes="F0_0" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_14" refDes="C535" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C548" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_1_0_F3_2_4" boardNumber="2" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="1_1_0_F1_5_2" boardNumber="2" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="1_1_0_F1_2_2" boardNumber="2" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C548" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C633" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C499" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C598" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_4" refDes="C671" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_1_1_F3_3_4" boardNumber="2" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="1_1_1_F3_1_4" boardNumber="2" refDes="F3_1" circuitNumber="4"/>
			<ReadFiducial refId="1_1_1_F1_6_2" boardNumber="2" refDes="F1_6" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C663" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C664" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C670" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C671" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C680" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C664" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C663" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C670" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C680" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_7" refDes="C499" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_1_2_F2_5_3" boardNumber="2" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="1_1_2_F3_0_4" boardNumber="2" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="1_1_2_F0_2_1" boardNumber="2" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C519" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C567" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C638" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C547" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_11" refDes="C671" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_1_3_F2_3_3" boardNumber="2" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="1_1_3_F2_1_3" boardNumber="2" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="1_1_3_F0_6_1" boardNumber="2" refDes="F0_6" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C603" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C664" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C663" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C670" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C671" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C680" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C664" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C663" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C670" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C680" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="3">
		<Index minBoardNumber="3" maxBoardNumber="3">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R246" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="2_2_0_F2_5_3" boardNumber="3" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="2_2_0_F0_3_1" boardNumber="3" refDes="F0_3" circuitNumber="1"/>
			<ReadFiducial refId="2_2_0_F0_2_1" boardNumber="3" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R221" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R241" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R256" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R301" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R226" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R241" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R282" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_17" refDes="R150" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="2_2_1_F2_4_3" boardNumber="3" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="2_2_1_F0_0_1" boardNumber="3" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="2_2_1_F2_6_3" boardNumber="3" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C483" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R251" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C531" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R253" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R235" circuitNumber="3" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C590" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R235" circuitNumber="1" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C610" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C483" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C507" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C588" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R253" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="3">
			<Pick boardNumber="2" alignWith="AM_6" refDes="R229" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="2_1_2_F3_5_4" boardNumber="2" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="2_1_2_F1_3_2" boardNumber="2" refDes="F1_3" circuitNumber="2"/>
			<ReadFiducial refId="2_1_2_F1_2_2" boardNumber="2" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R212" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R243" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R280" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R212" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="C631" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_10" refDes="R251" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="2_1_3_F3_4_4" boardNumber="2" refDes="F3_4" circuitNumber="4"/>
			<ReadFiducial refId="2_1_3_F1_6_2" boardNumber="2" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="2_1_3_F3_3_4" boardNumber="2" refDes="F3_3" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C507" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R150" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C550" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R154" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C582" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R235" circuitNumber="4" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C593" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R235" circuitNumber="2" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C610" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C528" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C551" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C586" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C608" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="4">
		<Index minBoardNumber="3" maxBoardNumber="3">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C515" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="3_2_0_F3_5_4" boardNumber="3" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="3_2_0_F1_6_2" boardNumber="3" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="3_2_0_F1_2_2" boardNumber="3" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C498" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C496" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C577" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C564" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C600" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C492" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C518" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C569" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_16" refDes="C614" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="3_2_1_F3_4_4" boardNumber="3" refDes="F3_4" circuitNumber="4"/>
			<ReadFiducial refId="3_2_1_F1_6_2" boardNumber="3" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="3_2_1_F0_2_1" boardNumber="3" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C505" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C584" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C477" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C651" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C578" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C554" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C509" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C614" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C529" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C477" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C613" circuitNumber="4" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C613" circuitNumber="2" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C623" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_19" refDes="C643" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="3_2_2_F2_5_3" boardNumber="3" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="3_2_2_F0_6_1" boardNumber="3" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="3_2_2_F0_2_1" boardNumber="3" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C492" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C521" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C569" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C597" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C640" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C492" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="3">
			<Pick boardNumber="2" alignWith="AM_9" refDes="C514" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="3_1_3_F3_2_4" boardNumber="2" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="3_1_3_F1_2_2" boardNumber="2" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_9" refDes="C578" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C613" circuitNumber="1" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="3_2_3_F2_1_3" boardNumber="3" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="3_2_3_F0_4_1" boardNumber="3" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="3_2_3_F0_1_1" boardNumber="3" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C508" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C491" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C527" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C665" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C477" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C551" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C614" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C613" circuitNumber="3" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C477" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C614" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="5">
		<Index minBoardNumber="4" maxBoardNumber="4">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R243" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_3_0_F2_5_3" boardNumber="4" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="4_3_0_F0_6_1" boardNumber="4" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="4_3_0_F2_1_3" boardNumber="4" refDes="F2_1" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C647" circuitNumber="3" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R220" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R244" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R280" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R212" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R221" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C647" circuitNumber="1" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="4">
			<Pick boardNumber="3" alignWith="AM_15" refDes="R281" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_2_1_F3_2_4" boardNumber="3" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="4_2_1_F1_5_2" boardNumber="3" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="4_2_1_F1_2_2" boardNumber="3" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R211" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R241" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R256" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R211" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R228" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R243" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R256" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R301" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C610" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_3_1_F2_4_3" boardNumber="4" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="4_3_1_F2_1_3" boardNumber="4" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="4_3_1_F0_0_1" boardNumber="4" refDes="F0_0" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C552" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C608" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_18" refDes="C647" circuitNumber="4" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_2_2_F3_5_4" boardNumber="3" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="4_2_2_F1_6_2" boardNumber="3" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="4_2_2_F3_1_4" boardNumber="3" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="R218" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C474" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="R231" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="R258" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C647" circuitNumber="2" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_21" refDes="R281" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_2_3_F2_5_3" boardNumber="3" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="4_2_3_F0_2_1" boardNumber="3" refDes="F0_2" circuitNumber="1"/>
			<ReadFiducial refId="4_2_3_F3_1_4" boardNumber="3" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C508" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R211" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R230" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R242" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R281" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R218" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R229" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R244" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R256" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R255" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R280" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R301" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="6">
		<Index minBoardNumber="4" maxBoardNumber="4">
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C662" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_3_0_F3_3_4" boardNumber="4" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="5_3_0_F3_1_4" boardNumber="4" refDes="F3_1" circuitNumber="4"/>
			<ReadFiducial refId="5_3_0_F1_6_2" boardNumber="4" refDes="F1_6" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C660" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C662" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C660" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C674" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C686" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C677" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_26" refDes="C567" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_3_1_F2_5_3" boardNumber="4" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="5_3_1_F3_0_4" boardNumber="4" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="5_3_1_F0_2_1" boardNumber="4" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C479" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C546" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C479" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C546" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C562" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C601" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C686" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C535" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C601" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_30" refDes="C674" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_3_2_F2_3_3" boardNumber="4" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="5_3_2_F0_6_1" boardNumber="4" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="5_3_2_F2_0_3" boardNumber="4" refDes="F2_0" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C660" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C499" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C398" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C660" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_33" refDes="C547" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_3_3_F2_4_3" boardNumber="4" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="5_3_3_F0_0_1" boardNumber="4" refDes="F0_0" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_33" refDes="C546" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_33" refDes="C479" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_33" refDes="C479" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_33" refDes="C546" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C562" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_2_3_F3_2_4" boardNumber="3" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="5_2_3_F1_5_2" boardNumber="3" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="5_2_3_F1_2_2" boardNumber="3" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C513" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C598" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C638" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C513" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C601" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="7">
		<Index minBoardNumber="4" maxBoardNumber="5">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="R306" circuitNumber="1" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="6_4_0_F2_4_3" boardNumber="5" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="6_4_0_F0_0_1" boardNumber="5" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="6_4_0_F2_6_3" boardNumber="5" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C651" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="R215" circuitNumber="3" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C645" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="R307" circuitNumber="3" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="R306" circuitNumber="3" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="R215" circuitNumber="1" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="R307" circuitNumber="1" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="5">
			<Pick boardNumber="4" alignWith="AM_25" refDes="C635" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="6_3_1_F3_5_4" boardNumber="4" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="6_3_1_F1_3_2" boardNumber="4" refDes="F1_3" circuitNumber="2"/>
			<ReadFiducial refId="6_3_1_F1_2_2" boardNumber="4" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C493" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C518" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C576" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C565" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C493" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C636" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C687" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C514" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C560" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C578" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C597" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C682" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_29" refDes="R306" circuitNumber="2" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="6_3_2_F3_3_4" boardNumber="4" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="6_3_2_F1_6_2" boardNumber="4" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="6_3_2_F3_4_4" boardNumber="4" refDes="F3_4" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C588" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="R215" circuitNumber="4" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="R307" circuitNumber="4" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="R306" circuitNumber="4" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="R215" circuitNumber="2" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="R307" circuitNumber="2" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_32" refDes="C635" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="6_3_3_F2_5_3" boardNumber="4" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="6_3_3_F0_3_1" boardNumber="4" refDes="F0_3" circuitNumber="1"/>
			<ReadFiducial refId="6_3_3_F0_2_1" boardNumber="4" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C637" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C476" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C518" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C581" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C631" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C685" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C505" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C538" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C564" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C578" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C631" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C685" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="8">
		<Index minBoardNumber="5" maxBoardNumber="5">
			<ToolbitExchange toolbitType="L5"/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="U37" circuitNumber="4" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="7_4_0_F3_4_4" boardNumber="5" refDes="F3_4" circuitNumber="4"/>
			<ReadFiducial refId="7_4_0_F1_1_2" boardNumber="5" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="7_4_0_F0_2_1" boardNumber="5" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="U37" circuitNumber="2" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C675" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C626" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C676" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="R261" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="7_4_1_F2_5_3" boardNumber="5" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="7_4_1_F0_6_1" boardNumber="5" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="7_4_1_F0_2_1" boardNumber="5" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C422" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="R261" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C646" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C540" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C563" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C577" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C576" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C646" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="5">
			<ToolbitExchange toolbitType="L5"/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="U37" circuitNumber="3" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="7_4_2_F2_1_3" boardNumber="5" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="7_4_2_F0_4_1" boardNumber="5" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="7_4_2_F0_1_1" boardNumber="5" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="U37" circuitNumber="1" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C643" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="7_3_3_F3_5_4" boardNumber="4" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="7_3_3_F1_6_2" boardNumber="4" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="7_3_3_F1_2_2" boardNumber="4" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="R261" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C568" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="R261" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C643" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C672" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C540" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C566" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C577" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C632" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C642" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="9">
		<Index minBoardNumber="5" maxBoardNumber="6">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C667" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="8_5_0_F2_4_3" boardNumber="6" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="8_5_0_F2_1_3" boardNumber="6" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="8_5_0_F0_0_1" boardNumber="6" refDes="F0_0" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="R240" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C545" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C555" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="R240" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_34" refDes="C538" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="8_4_0_F3_2_4" boardNumber="5" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="8_4_0_F1_5_2" boardNumber="5" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="8_4_0_F1_2_2" boardNumber="5" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_34" refDes="C505" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_34" refDes="C631" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_37" refDes="C628" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="8_4_1_F3_5_4" boardNumber="5" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="8_4_1_F1_6_2" boardNumber="5" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="8_4_1_F3_1_4" boardNumber="5" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C501" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C604" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C569" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C665" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C640" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C474" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C497" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C564" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C582" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C600" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C636" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C667" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_40" refDes="R240" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="8_4_2_F2_5_3" boardNumber="5" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="8_4_2_F0_2_1" boardNumber="5" refDes="F0_2" circuitNumber="1"/>
			<ReadFiducial refId="8_4_2_F3_1_4" boardNumber="5" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C563" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="R240" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C641" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C531" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C590" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C589" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_43" refDes="C636" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="8_4_3_F2_5_3" boardNumber="5" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="8_4_3_F0_6_1" boardNumber="5" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="8_4_3_F2_1_3" boardNumber="5" refDes="F2_1" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C648" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C497" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C515" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C582" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C606" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C687" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C476" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C501" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C565" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C582" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C606" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C640" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="10">
		<Index minBoardNumber="6" maxBoardNumber="6">
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C562" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_5_0_F2_5_3" boardNumber="6" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="9_5_0_F3_0_4" boardNumber="6" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="9_5_0_F0_2_1" boardNumber="6" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="FB1" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C547" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="FB1" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C598" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C683" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C519" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C598" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_47" refDes="C669" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_5_1_F2_3_3" boardNumber="6" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="9_5_1_F0_6_1" boardNumber="6" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="9_5_1_F2_0_3" boardNumber="6" refDes="F2_0" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C583" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C522" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C484" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C583" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C662" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C484" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C677" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C677" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_50" refDes="FB1" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_5_2_F2_4_3" boardNumber="6" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="9_5_2_F0_0_1" boardNumber="6" refDes="F0_0" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_50" refDes="FB1" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C562" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_4_2_F3_2_4" boardNumber="5" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="9_4_2_F1_5_2" boardNumber="5" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="9_4_2_F1_2_2" boardNumber="5" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C494" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C601" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_42" refDes="C611" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_4_3_F3_3_4" boardNumber="5" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="9_4_3_F3_1_4" boardNumber="5" refDes="F3_1" circuitNumber="4"/>
			<ReadFiducial refId="9_4_3_F1_6_2" boardNumber="5" refDes="F1_6" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C499" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C484" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C583" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C603" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C583" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C673" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C484" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C677" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C603" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C673" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="11">
		<Index minBoardNumber="6" maxBoardNumber="7">
			<ToolbitExchange toolbitType="CPL4"/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q19" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="10_5_1_F3_6_4" boardNumber="6" refDes="F3_6" circuitNumber="4"/>
			<ReadFiducial refId="10_5_1_F1_0_2" boardNumber="6" refDes="F1_0" circuitNumber="2"/>
			<ReadFiducial refId="10_5_1_F3_4_4" boardNumber="6" refDes="F3_4" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="C622" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q17" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="C661" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q19" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="C627" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q17" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="C541" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="D13" circuitNumber="4" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="C678" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="D13" circuitNumber="2" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="C661" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_51" refDes="C534" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="10_5_2_F2_5_3" boardNumber="6" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="10_5_2_F0_3_1" boardNumber="6" refDes="F0_3" circuitNumber="1"/>
			<ReadFiducial refId="10_5_2_F0_2_1" boardNumber="6" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_51" refDes="C525" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_51" refDes="C525" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q19" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="10_5_3_F2_6_3" boardNumber="6" refDes="F2_6" circuitNumber="3"/>
			<ReadFiducial refId="10_5_3_F0_0_1" boardNumber="6" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="10_5_3_F2_4_3" boardNumber="6" refDes="F2_4" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C524" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C661" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q17" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C525" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q19" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C619" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q17" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C627" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="D13" circuitNumber="3" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C649" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="D13" circuitNumber="1" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C525" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C661" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="12">
		<Index minBoardNumber="7" maxBoardNumber="7">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C521" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="11_6_0_F2_5_3" boardNumber="7" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="11_6_0_F0_6_1" boardNumber="7" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="11_6_0_F0_2_1" boardNumber="7" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C496" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C564" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C592" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C635" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C472" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C496" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C569" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="7">
			<ToolbitExchange toolbitType="L7"/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="L9" circuitNumber="3" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="11_6_1_F2_1_3" boardNumber="7" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="11_6_1_F0_4_1" boardNumber="7" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="11_6_1_F0_1_1" boardNumber="7" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="L9" circuitNumber="1" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="R247" circuitNumber="1" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C542" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C542" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="R250" circuitNumber="1" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_52" refDes="C496" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="11_5_2_F3_5_4" boardNumber="6" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="11_5_2_F1_6_2" boardNumber="6" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="11_5_2_F1_2_2" boardNumber="6" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C521" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C581" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C682" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C521" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_54" refDes="R250" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="11_5_3_F3_4_4" boardNumber="6" refDes="F3_4" circuitNumber="4"/>
			<ReadFiducial refId="11_5_3_F1_1_2" boardNumber="6" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="11_5_3_F0_2_1" boardNumber="6" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C551" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C542" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="R225" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C606" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="R234" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="R250" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C610" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="L7"/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="L9" circuitNumber="4" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="L9" circuitNumber="2" feederNumber="27" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="13">
		<Index minBoardNumber="7" maxBoardNumber="7">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C604" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="12_6_0_F3_1_4" boardNumber="7" refDes="F3_1" circuitNumber="4"/>
			<ReadFiducial refId="12_6_0_F1_6_2" boardNumber="7" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="12_6_0_F3_5_4" boardNumber="7" refDes="F3_5" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C502" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C639" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C667" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C503" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C502" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C646" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="7">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="U43" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="12_6_1_F2_5_3" boardNumber="7" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="12_6_1_F0_2_1" boardNumber="7" refDes="F0_2" circuitNumber="1"/>
			<ReadFiducial refId="12_6_1_F3_1_4" boardNumber="7" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="U36" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="U41" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="U43" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="U48" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="U53" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="U36" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="U41" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="U48" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="7">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C594" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="12_6_2_F2_1_3" boardNumber="7" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="12_6_2_F0_6_1" boardNumber="7" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="12_6_2_F2_5_3" boardNumber="7" refDes="F2_5" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C604" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C679" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C500" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C634" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="7">
			<Pick boardNumber="7" alignWith="AM_63" refDes="C554" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="12_6_3_F2_4_3" boardNumber="7" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="12_6_3_F2_1_3" boardNumber="7" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="12_6_3_F0_0_1" boardNumber="7" refDes="F0_0" circuitNumber="1"/>
			<Place/>
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U48" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U36" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U41" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U48" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U36" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U41" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U43" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U53" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="14">
		<Index minBoardNumber="8" maxBoardNumber="8">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="U39" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="13_7_0_F2_3_3" boardNumber="8" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="13_7_0_F0_6_1" boardNumber="8" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="13_7_0_F2_0_3" boardNumber="8" refDes="F2_0" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="U46" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="U50" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="U24" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="U35" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="U24" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="8">
			<Pick boardNumber="7" alignWith="AM_60" refDes="U47" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="13_6_1_F3_2_4" boardNumber="7" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="13_6_1_F1_5_2" boardNumber="7" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="13_6_1_F1_2_2" boardNumber="7" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U40" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U39" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U50" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U55" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U34" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U39" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U46" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U54" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="7">
			<Pick boardNumber="7" alignWith="AM_62" refDes="U24" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="13_6_2_F3_3_4" boardNumber="7" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="13_6_2_F1_6_2" boardNumber="7" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="13_6_2_F3_0_4" boardNumber="7" refDes="F3_0" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="U35" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="U24" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="7">
			<Pick boardNumber="7" alignWith="AM_64" refDes="U58" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="13_6_3_F2_5_3" boardNumber="7" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="13_6_3_F3_0_4" boardNumber="7" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="13_6_3_F0_2_1" boardNumber="7" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U49" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U55" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U38" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U47" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U50" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U49" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U55" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U54" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U59" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="15">
		<Index minBoardNumber="8" maxBoardNumber="8">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R262" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="14_7_0_F3_4_4" boardNumber="8" refDes="F3_4" circuitNumber="4"/>
			<ReadFiducial refId="14_7_0_F1_0_2" boardNumber="8" refDes="F1_0" circuitNumber="2"/>
			<ReadFiducial refId="14_7_0_F3_6_4" boardNumber="8" refDes="F3_6" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R213" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R263" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R239" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R184" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U46" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="14_7_1_F2_5_3" boardNumber="8" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="14_7_1_F0_3_1" boardNumber="8" refDes="F0_3" circuitNumber="1"/>
			<ReadFiducial refId="14_7_1_F0_2_1" boardNumber="8" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U38" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U54" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U59" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U58" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U40" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U45" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="R239" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="8">
			<Pick boardNumber="8" alignWith="AM_72" refDes="R213" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="14_7_2_F2_4_3" boardNumber="8" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="14_7_2_F0_0_1" boardNumber="8" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="14_7_2_F2_6_3" boardNumber="8" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R239" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R262" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R184" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R303" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="8">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U58" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="14_6_3_F3_5_4" boardNumber="7" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="14_6_3_F1_3_2" boardNumber="7" refDes="F1_3" circuitNumber="2"/>
			<ReadFiducial refId="14_6_3_F1_2_2" boardNumber="7" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U34" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U45" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U54" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U59" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U38" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U45" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U49" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U55" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U59" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="16">
		<Index minBoardNumber="8" maxBoardNumber="9">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="8" alignWith="AM_68" refDes="R242" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="15_7_0_F3_2_4" boardNumber="8" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="15_7_0_F1_2_2" boardNumber="8" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R263" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="15_8_0_F2_1_3" boardNumber="9" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="15_8_0_F0_4_1" boardNumber="9" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="15_8_0_F0_1_1" boardNumber="9" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R213" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R263" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R239" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R262" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R184" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<Pick boardNumber="8" alignWith="AM_71" refDes="R284" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="15_7_1_F3_5_4" boardNumber="8" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="15_7_1_F1_6_2" boardNumber="8" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="15_7_1_F1_2_2" boardNumber="8" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R229" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R284" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="U46" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="U47" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="U50" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<Pick boardNumber="8" alignWith="AM_73" refDes="U51" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="15_7_2_F3_4_4" boardNumber="8" refDes="F3_4" circuitNumber="4"/>
			<ReadFiducial refId="15_7_2_F1_1_2" boardNumber="8" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="15_7_2_F0_2_1" boardNumber="8" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="U40" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="U47" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="U51" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<Pick boardNumber="8" alignWith="AM_75" refDes="U34" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="15_7_3_F2_3_3" boardNumber="8" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="15_7_3_F0_6_1" boardNumber="8" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="15_7_3_F0_2_1" boardNumber="8" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U35" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U39" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U51" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U51" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="R303" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="R284" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="R284" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="17">
		<Index minBoardNumber="9" maxBoardNumber="9">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="U53" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="16_8_0_F2_5_3" boardNumber="9" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="16_8_0_F0_2_1" boardNumber="9" refDes="F0_2" circuitNumber="1"/>
			<ReadFiducial refId="16_8_0_F3_1_4" boardNumber="9" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C632" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C545" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C510" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="9">
			<Pick boardNumber="9" alignWith="AM_80" refDes="C666" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="16_8_1_F2_1_3" boardNumber="9" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="16_8_1_F0_6_1" boardNumber="9" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="16_8_1_F2_5_3" boardNumber="9" refDes="F2_5" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C495" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C628" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C656" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C503" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C495" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C604" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C648" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C656" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C665" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C667" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C679" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="9">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="U43" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="16_8_2_F2_4_3" boardNumber="9" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="16_8_2_F2_1_3" boardNumber="9" refDes="F2_1" circuitNumber="3"/>
			<ReadFiducial refId="16_8_2_F0_0_1" boardNumber="9" refDes="F0_0" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="U53" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C666" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="16_7_3_F3_1_4" boardNumber="8" refDes="F3_1" circuitNumber="4"/>
			<ReadFiducial refId="16_7_3_F1_6_2" boardNumber="8" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="16_7_3_F3_5_4" boardNumber="8" refDes="F3_5" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C563" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C646" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C666" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C498" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C584" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C644" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C648" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C657" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C665" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C672" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="18">
		<Index minBoardNumber="9" maxBoardNumber="10">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U35" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="17_8_0_F3_2_4" boardNumber="9" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="17_8_0_F1_5_2" boardNumber="9" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="17_8_0_F1_2_2" boardNumber="9" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U38" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U49" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U58" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U40" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="9">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C658" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="17_8_1_F3_3_4" boardNumber="9" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="17_8_1_F1_6_2" boardNumber="9" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="17_8_1_F3_0_4" boardNumber="9" refDes="F3_0" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R285" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C554" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R305" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C555" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R264" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C609" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R285" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C657" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C530" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R305" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="9">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="U34" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="17_8_2_F2_5_3" boardNumber="9" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="17_8_2_F3_0_4" boardNumber="9" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="17_8_2_F0_2_1" boardNumber="9" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="U45" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="9">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C657" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="17_8_3_F2_3_3" boardNumber="9" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="17_8_3_F0_6_1" boardNumber="9" refDes="F0_6" circuitNumber="1"/>
			<ReadFiducial refId="17_8_3_F2_0_3" boardNumber="9" refDes="F2_0" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R264" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C589" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R285" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C607" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R305" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C657" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R264" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C511" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C607" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R305" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="19">
		<Index minBoardNumber="10" maxBoardNumber="10">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="R270" circuitNumber="2" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="18_9_0_F0_3_1" boardNumber="10" refDes="F0_3" circuitNumber="1"/>
			<ReadFiducial refId="18_9_0_F3_5_4" boardNumber="10" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="18_9_0_F1_2_2" boardNumber="10" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="C644" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="R269" circuitNumber="4" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="R270" circuitNumber="4" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="R269" circuitNumber="2" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="C637" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="10">
			<ToolbitExchange toolbitType="CPL4"/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="Q21" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="18_9_1_F0_0_1" boardNumber="10" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="18_9_1_F2_5_3" boardNumber="10" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="18_9_1_F1_1_2" boardNumber="10" refDes="F1_1" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="Q20" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="Q20" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="Q21" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="Q21" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="Q20" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="Q20" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="Q21" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="10">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="10" alignWith="AM_90" refDes="C510" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="18_9_2_F0_4_1" boardNumber="10" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="18_9_2_F2_5_3" boardNumber="10" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="18_9_2_F0_2_1" boardNumber="10" refDes="F0_2" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_90" refDes="R269" circuitNumber="3" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="10">
			<Pick boardNumber="10" alignWith="AM_91" refDes="R269" circuitNumber="1" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="18_9_3_F0_4_1" boardNumber="10" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="18_9_3_F0_1_1" boardNumber="10" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="18_9_3_F2_1_3" boardNumber="10" refDes="F2_1" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_91" refDes="R270" circuitNumber="3" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_91" refDes="R270" circuitNumber="1" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="L7"/>
			<Pick boardNumber="10" alignWith="AM_91" refDes="VR57" circuitNumber="3" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_91" refDes="VR57" circuitNumber="1" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_86" refDes="VR57" circuitNumber="4" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="18_8_3_F3_5_4" boardNumber="9" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="18_8_3_F1_2_2" boardNumber="9" refDes="F1_2" circuitNumber="2"/>
			<ReadFiducial refId="18_8_3_F1_4_2" boardNumber="9" refDes="F1_4" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_86" refDes="VR57" circuitNumber="2" feederNumber="19" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
</PlacementProgram>
