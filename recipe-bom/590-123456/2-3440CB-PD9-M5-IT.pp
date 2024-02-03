<?xml version="1.0"?>
<PlacementProgram xmlns="http://api.assembleon.com/pp/v2">
	<General syntaxVersion="2.0" lastModified="2022-11-26T15:19:59Z" positionInLine="2" machineModel="AX-501" operationMode="LOCAL" cycleTime="50883">
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
	<Board id="3440CB-PD9-M5-IT" length="269.737" width="220.917" thickness="1.250" cadOriginX="0.000" cadOriginY="0.000" handlingClass="HIGH" maxComponentHeightNonProducingSide="20.000">
		<Badmark modelId="r1574.8" x="-185.138" y="107.459" z="0.000" rz="0.00" refDes="B0" circuitNumber="1"/>
		<Badmark modelId="r1574.8" x="-48.998" y="107.459" z="0.000" rz="0.00" refDes="B1" circuitNumber="2"/>
		<Badmark modelId="r1574.8" x="-185.138" y="212.259" z="0.000" rz="0.00" refDes="B2" circuitNumber="3"/>
		<Badmark modelId="r1574.8" x="-48.998" y="212.259" z="0.000" rz="0.00" refDes="B3" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-256.838" y="53.959" z="0.000" rz="0.00" refDes="F0_0" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-151.838" y="19.359" z="0.000" rz="0.00" refDes="F0_1" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-262.058" y="92.659" z="0.000" rz="0.00" refDes="F0_2" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-238.038" y="60.859" z="0.000" rz="0.00" refDes="F0_3" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-195.188" y="102.659" z="0.000" rz="0.00" refDes="F0_4" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-220.238" y="19.409" z="0.000" rz="0.00" refDes="F0_5" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-167.638" y="107.459" z="0.000" rz="0.00" refDes="F0_6" circuitNumber="1"/>
		<Fiducial modelId="r1016" x="-120.698" y="53.959" z="0.000" rz="0.00" refDes="F1_0" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-15.698" y="19.359" z="0.000" rz="0.00" refDes="F1_1" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-125.918" y="92.659" z="0.000" rz="0.00" refDes="F1_2" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-101.898" y="60.859" z="0.000" rz="0.00" refDes="F1_3" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-59.048" y="102.659" z="0.000" rz="0.00" refDes="F1_4" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-84.098" y="19.409" z="0.000" rz="0.00" refDes="F1_5" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-31.498" y="107.459" z="0.000" rz="0.00" refDes="F1_6" circuitNumber="2"/>
		<Fiducial modelId="r1016" x="-256.838" y="158.759" z="0.000" rz="0.00" refDes="F2_0" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-151.838" y="124.159" z="0.000" rz="0.00" refDes="F2_1" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-262.058" y="197.459" z="0.000" rz="0.00" refDes="F2_2" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-238.038" y="165.659" z="0.000" rz="0.00" refDes="F2_3" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-195.188" y="207.459" z="0.000" rz="0.00" refDes="F2_4" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-220.238" y="124.209" z="0.000" rz="0.00" refDes="F2_5" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-167.638" y="212.259" z="0.000" rz="0.00" refDes="F2_6" circuitNumber="3"/>
		<Fiducial modelId="r1016" x="-120.698" y="158.759" z="0.000" rz="0.00" refDes="F3_0" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-15.698" y="124.159" z="0.000" rz="0.00" refDes="F3_1" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-125.918" y="197.459" z="0.000" rz="0.00" refDes="F3_2" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-101.898" y="165.659" z="0.000" rz="0.00" refDes="F3_3" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-59.048" y="207.459" z="0.000" rz="0.00" refDes="F3_4" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-84.098" y="124.209" z="0.000" rz="0.00" refDes="F3_5" circuitNumber="4"/>
		<Fiducial modelId="r1016" x="-31.498" y="212.259" z="0.000" rz="0.00" refDes="F3_6" circuitNumber="4"/>
		<AlignmentMatrix id="AM_1" type="CIRCUIT">
			<FiducialReference refId="0_0_3_F3_3_4" refDes="F3_3" circuitNumber="4"/>
			<FiducialReference refId="0_0_3_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="0_0_3_F1_6_2" refDes="F1_6" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_10" type="CIRCUIT">
			<FiducialReference refId="2_1_3_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="2_1_3_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="2_1_3_F3_4_4" refDes="F3_4" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_11" type="CIRCUIT">
			<FiducialReference refId="1_1_3_F2_3_3" refDes="F2_3" circuitNumber="3"/>
			<FiducialReference refId="1_1_3_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="1_1_3_F2_4_3" refDes="F2_4" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_12" type="CIRCUIT">
			<FiducialReference refId="3_2_0_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="3_2_0_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="3_2_0_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_13" type="CIRCUIT">
			<FiducialReference refId="2_2_0_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="2_2_0_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="2_2_0_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_14" type="CIRCUIT">
			<FiducialReference refId="1_2_0_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="1_2_0_F2_2_3" refDes="F2_2" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_15" type="CIRCUIT">
			<FiducialReference refId="4_2_1_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="4_2_1_F1_1_2" refDes="F1_1" circuitNumber="2"/>
			<FiducialReference refId="4_2_1_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_16" type="CIRCUIT">
			<FiducialReference refId="3_2_1_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="3_2_1_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="3_2_1_F1_5_2" refDes="F1_5" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_17" type="CIRCUIT">
			<FiducialReference refId="2_2_1_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="2_2_1_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="2_2_1_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_18" type="CIRCUIT">
			<FiducialReference refId="4_2_2_F3_3_4" refDes="F3_3" circuitNumber="4"/>
			<FiducialReference refId="4_2_2_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="4_2_2_F1_6_2" refDes="F1_6" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_19" type="CIRCUIT">
			<FiducialReference refId="3_2_2_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="3_2_2_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="3_2_2_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_2" type="CIRCUIT">
			<FiducialReference refId="1_1_0_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="1_1_0_F1_1_2" refDes="F1_1" circuitNumber="2"/>
			<FiducialReference refId="1_1_0_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_20" type="CIRCUIT">
			<FiducialReference refId="5_2_3_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="5_2_3_F1_1_2" refDes="F1_1" circuitNumber="2"/>
			<FiducialReference refId="5_2_3_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_21" type="CIRCUIT">
			<FiducialReference refId="4_2_3_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="4_2_3_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="4_2_3_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_22" type="CIRCUIT">
			<FiducialReference refId="3_2_3_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="3_2_3_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="3_2_3_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_23" type="CIRCUIT">
			<FiducialReference refId="5_3_0_F3_3_4" refDes="F3_3" circuitNumber="4"/>
			<FiducialReference refId="5_3_0_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="5_3_0_F3_4_4" refDes="F3_4" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_24" type="CIRCUIT">
			<FiducialReference refId="4_3_0_F2_3_3" refDes="F2_3" circuitNumber="3"/>
			<FiducialReference refId="4_3_0_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="4_3_0_F0_6_1" refDes="F0_6" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_25" type="CIRCUIT">
			<FiducialReference refId="6_3_1_F1_4_2" refDes="F1_4" circuitNumber="2"/>
			<FiducialReference refId="6_3_1_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="6_3_1_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_26" type="CIRCUIT">
			<FiducialReference refId="5_3_1_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="5_3_1_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="5_3_1_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_27" type="CIRCUIT">
			<FiducialReference refId="4_3_1_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="4_3_1_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="4_3_1_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_29" type="CIRCUIT">
			<FiducialReference refId="6_3_2_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="6_3_2_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="6_3_2_F3_4_4" refDes="F3_4" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_3" type="CIRCUIT">
			<FiducialReference refId="0_1_0_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="0_1_0_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="0_1_0_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_30" type="CIRCUIT">
			<FiducialReference refId="5_3_2_F2_0_3" refDes="F2_0" circuitNumber="3"/>
			<FiducialReference refId="5_3_2_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="5_3_2_F2_4_3" refDes="F2_4" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_31" type="CIRCUIT">
			<FiducialReference refId="7_3_3_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="7_3_3_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="7_3_3_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_32" type="CIRCUIT">
			<FiducialReference refId="6_3_3_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="6_3_3_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="6_3_3_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_33" type="CIRCUIT">
			<FiducialReference refId="5_3_3_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="5_3_3_F2_2_3" refDes="F2_2" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_35" type="CIRCUIT">
			<FiducialReference refId="7_4_0_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="7_4_0_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="7_4_0_F1_5_2" refDes="F1_5" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_36" type="CIRCUIT">
			<FiducialReference refId="6_4_0_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="6_4_0_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="6_4_0_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_37" type="CIRCUIT">
			<FiducialReference refId="8_4_1_F3_3_4" refDes="F3_3" circuitNumber="4"/>
			<FiducialReference refId="8_4_1_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="8_4_1_F1_6_2" refDes="F1_6" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_38" type="CIRCUIT">
			<FiducialReference refId="7_4_1_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="7_4_1_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="7_4_1_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_39" type="CIRCUIT">
			<FiducialReference refId="9_4_2_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="9_4_2_F1_1_2" refDes="F1_1" circuitNumber="2"/>
			<FiducialReference refId="9_4_2_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_4" type="CIRCUIT">
			<FiducialReference refId="1_1_1_F3_3_4" refDes="F3_3" circuitNumber="4"/>
			<FiducialReference refId="1_1_1_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="1_1_1_F3_4_4" refDes="F3_4" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_40" type="CIRCUIT">
			<FiducialReference refId="8_4_2_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="8_4_2_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="8_4_2_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_41" type="CIRCUIT">
			<FiducialReference refId="7_4_2_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="7_4_2_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="7_4_2_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_42" type="CIRCUIT">
			<FiducialReference refId="9_4_3_F3_3_4" refDes="F3_3" circuitNumber="4"/>
			<FiducialReference refId="9_4_3_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="9_4_3_F3_4_4" refDes="F3_4" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_43" type="CIRCUIT">
			<FiducialReference refId="8_4_3_F2_3_3" refDes="F2_3" circuitNumber="3"/>
			<FiducialReference refId="8_4_3_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="8_4_3_F2_6_3" refDes="F2_6" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_44" type="CIRCUIT">
			<FiducialReference refId="9_5_0_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="9_5_0_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="9_5_0_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_45" type="CIRCUIT">
			<FiducialReference refId="10_5_0_F1_4_2" refDes="F1_4" circuitNumber="2"/>
			<FiducialReference refId="10_5_0_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="10_5_0_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_46" type="CIRCUIT">
			<FiducialReference refId="8_5_0_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="8_5_0_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="8_5_0_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_47" type="CIRCUIT">
			<FiducialReference refId="9_5_1_F2_0_3" refDes="F2_0" circuitNumber="3"/>
			<FiducialReference refId="9_5_1_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="9_5_1_F2_4_3" refDes="F2_4" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_48" type="CIRCUIT">
			<FiducialReference refId="10_5_1_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="10_5_1_F1_0_2" refDes="F1_0" circuitNumber="2"/>
			<FiducialReference refId="10_5_1_F3_5_4" refDes="F3_5" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_5" type="CIRCUIT">
			<FiducialReference refId="0_1_1_F2_3_3" refDes="F2_3" circuitNumber="3"/>
			<FiducialReference refId="0_1_1_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="0_1_1_F0_6_1" refDes="F0_6" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_50" type="CIRCUIT">
			<FiducialReference refId="9_5_2_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="9_5_2_F0_0_1" refDes="F0_0" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_51" type="CIRCUIT">
			<FiducialReference refId="10_5_2_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="10_5_2_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="10_5_2_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_52" type="CIRCUIT">
			<FiducialReference refId="11_5_2_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="11_5_2_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="11_5_2_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_53" type="CIRCUIT">
			<FiducialReference refId="10_5_3_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="10_5_3_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="10_5_3_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_54" type="CIRCUIT">
			<FiducialReference refId="11_5_3_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="11_5_3_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="11_5_3_F1_5_2" refDes="F1_5" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_56" type="CIRCUIT">
			<FiducialReference refId="11_6_0_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="11_6_0_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="11_6_0_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_57" type="CIRCUIT">
			<FiducialReference refId="12_6_0_F3_3_4" refDes="F3_3" circuitNumber="4"/>
			<FiducialReference refId="12_6_0_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="12_6_0_F3_6_4" refDes="F3_6" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_58" type="CIRCUIT">
			<FiducialReference refId="11_6_1_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="11_6_1_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="11_6_1_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_59" type="CIRCUIT">
			<FiducialReference refId="12_6_1_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="12_6_1_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="12_6_1_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_6" type="CIRCUIT">
			<FiducialReference refId="2_1_2_F1_4_2" refDes="F1_4" circuitNumber="2"/>
			<FiducialReference refId="2_1_2_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="2_1_2_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_60" type="CIRCUIT">
			<FiducialReference refId="13_6_1_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="13_6_1_F1_1_2" refDes="F1_1" circuitNumber="2"/>
			<FiducialReference refId="13_6_1_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_61" type="CIRCUIT">
			<FiducialReference refId="12_6_2_F2_3_3" refDes="F2_3" circuitNumber="3"/>
			<FiducialReference refId="12_6_2_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="12_6_2_F2_6_3" refDes="F2_6" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_62" type="CIRCUIT">
			<FiducialReference refId="13_6_2_F3_0_4" refDes="F3_0" circuitNumber="4"/>
			<FiducialReference refId="13_6_2_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="13_6_2_F3_4_4" refDes="F3_4" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_63" type="CIRCUIT">
			<FiducialReference refId="12_6_3_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="12_6_3_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="12_6_3_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_64" type="CIRCUIT">
			<FiducialReference refId="13_6_3_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="13_6_3_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="13_6_3_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_65" type="CIRCUIT">
			<FiducialReference refId="14_6_3_F1_4_2" refDes="F1_4" circuitNumber="2"/>
			<FiducialReference refId="14_6_3_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="14_6_3_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_66" type="CIRCUIT">
			<FiducialReference refId="13_7_0_F2_0_3" refDes="F2_0" circuitNumber="3"/>
			<FiducialReference refId="13_7_0_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="13_7_0_F2_4_3" refDes="F2_4" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_67" type="CIRCUIT">
			<FiducialReference refId="14_7_0_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="14_7_0_F1_0_2" refDes="F1_0" circuitNumber="2"/>
			<FiducialReference refId="14_7_0_F3_5_4" refDes="F3_5" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_7" type="CIRCUIT">
			<FiducialReference refId="1_1_2_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="1_1_2_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="1_1_2_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_70" type="CIRCUIT">
			<FiducialReference refId="14_7_1_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="14_7_1_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="14_7_1_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_71" type="CIRCUIT">
			<FiducialReference refId="15_7_1_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="15_7_1_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="15_7_1_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_72" type="CIRCUIT">
			<FiducialReference refId="14_7_2_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="14_7_2_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="14_7_2_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_73" type="CIRCUIT">
			<FiducialReference refId="15_7_2_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="15_7_2_F3_2_4" refDes="F3_2" circuitNumber="4"/>
			<FiducialReference refId="15_7_2_F1_5_2" refDes="F1_5" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_75" type="CIRCUIT">
			<FiducialReference refId="15_7_3_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="15_7_3_F2_4_3" refDes="F2_4" circuitNumber="3"/>
			<FiducialReference refId="15_7_3_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_76" type="CIRCUIT">
			<FiducialReference refId="16_7_3_F3_3_4" refDes="F3_3" circuitNumber="4"/>
			<FiducialReference refId="16_7_3_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="16_7_3_F3_6_4" refDes="F3_6" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_77" type="CIRCUIT">
			<FiducialReference refId="15_8_0_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="15_8_0_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="15_8_0_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_78" type="CIRCUIT">
			<FiducialReference refId="16_8_0_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="16_8_0_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="16_8_0_F3_3_4" refDes="F3_3" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_79" type="CIRCUIT">
			<FiducialReference refId="17_8_0_F1_6_2" refDes="F1_6" circuitNumber="2"/>
			<FiducialReference refId="17_8_0_F1_1_2" refDes="F1_1" circuitNumber="2"/>
			<FiducialReference refId="17_8_0_F3_1_4" refDes="F3_1" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_8" type="CIRCUIT">
			<FiducialReference refId="0_1_2_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="0_1_2_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="0_1_2_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_80" type="CIRCUIT">
			<FiducialReference refId="16_8_1_F2_3_3" refDes="F2_3" circuitNumber="3"/>
			<FiducialReference refId="16_8_1_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="16_8_1_F2_6_3" refDes="F2_6" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_81" type="CIRCUIT">
			<FiducialReference refId="17_8_1_F3_0_4" refDes="F3_0" circuitNumber="4"/>
			<FiducialReference refId="17_8_1_F1_5_2" refDes="F1_5" circuitNumber="2"/>
			<FiducialReference refId="17_8_1_F3_4_4" refDes="F3_4" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_82" type="CIRCUIT">
			<FiducialReference refId="16_8_2_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="16_8_2_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="16_8_2_F2_3_3" refDes="F2_3" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_83" type="CIRCUIT">
			<FiducialReference refId="17_8_2_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="17_8_2_F0_1_1" refDes="F0_1" circuitNumber="1"/>
			<FiducialReference refId="17_8_2_F3_0_4" refDes="F3_0" circuitNumber="4"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_85" type="CIRCUIT">
			<FiducialReference refId="17_8_3_F2_0_3" refDes="F2_0" circuitNumber="3"/>
			<FiducialReference refId="17_8_3_F0_5_1" refDes="F0_5" circuitNumber="1"/>
			<FiducialReference refId="17_8_3_F2_4_3" refDes="F2_4" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_86" type="CIRCUIT">
			<FiducialReference refId="18_8_3_F1_2_2" refDes="F1_2" circuitNumber="2"/>
			<FiducialReference refId="18_8_3_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="18_8_3_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_87" type="CIRCUIT">
			<FiducialReference refId="17_9_0_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="17_9_0_F2_2_3" refDes="F2_2" circuitNumber="3"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_88" type="CIRCUIT">
			<FiducialReference refId="18_9_0_F0_4_1" refDes="F0_4" circuitNumber="1"/>
			<FiducialReference refId="18_9_0_F3_6_4" refDes="F3_6" circuitNumber="4"/>
			<FiducialReference refId="18_9_0_F1_1_2" refDes="F1_1" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_89" type="CIRCUIT">
			<FiducialReference refId="18_9_1_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="18_9_1_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="18_9_1_F1_5_2" refDes="F1_5" circuitNumber="2"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_90" type="CIRCUIT">
			<FiducialReference refId="18_9_2_F0_2_1" refDes="F0_2" circuitNumber="1"/>
			<FiducialReference refId="18_9_2_F2_6_3" refDes="F2_6" circuitNumber="3"/>
			<FiducialReference refId="18_9_2_F0_1_1" refDes="F0_1" circuitNumber="1"/>
		</AlignmentMatrix>
		<AlignmentMatrix id="AM_91" type="CIRCUIT">
			<FiducialReference refId="18_9_3_F2_2_3" refDes="F2_2" circuitNumber="3"/>
			<FiducialReference refId="18_9_3_F0_0_1" refDes="F0_0" circuitNumber="1"/>
			<FiducialReference refId="18_9_3_F2_5_3" refDes="F2_5" circuitNumber="3"/>
		</AlignmentMatrix>
		<Component partNumber="510-500719" refDes="R114" circuitNumber="1" x="-259.738" y="63.509" z="0.000" rz="0.00"/>
		<Component partNumber="510-500719" refDes="R114" circuitNumber="2" x="-123.598" y="63.509" z="0.000" rz="0.00"/>
		<Component partNumber="510-500719" refDes="R114" circuitNumber="3" x="-259.738" y="168.309" z="0.000" rz="0.00"/>
		<Component partNumber="510-500719" refDes="R114" circuitNumber="4" x="-123.598" y="168.309" z="0.000" rz="0.00"/>
		<Component partNumber="510-500866" refDes="R210" circuitNumber="1" x="-212.368" y="13.709" z="0.000" rz="0.00"/>
		<Component partNumber="510-500866" refDes="R210" circuitNumber="2" x="-76.228" y="13.709" z="0.000" rz="0.00"/>
		<Component partNumber="510-500866" refDes="R210" circuitNumber="3" x="-212.368" y="118.509" z="0.000" rz="0.00"/>
		<Component partNumber="510-500866" refDes="R210" circuitNumber="4" x="-76.228" y="118.509" z="0.000" rz="0.00"/>
		<Component partNumber="510-500878" refDes="R101" circuitNumber="1" x="-240.668" y="76.078" z="0.000" rz="180.00"/>
		<Component partNumber="510-500878" refDes="R101" circuitNumber="2" x="-104.528" y="76.078" z="0.000" rz="180.00"/>
		<Component partNumber="510-500878" refDes="R101" circuitNumber="3" x="-240.668" y="180.878" z="0.000" rz="180.00"/>
		<Component partNumber="510-500878" refDes="R101" circuitNumber="4" x="-104.528" y="180.878" z="0.000" rz="180.00"/>
		<Component partNumber="510-500881" refDes="R107" circuitNumber="1" x="-246.028" y="74.492" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R107" circuitNumber="2" x="-109.888" y="74.492" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R107" circuitNumber="3" x="-246.028" y="179.292" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R107" circuitNumber="4" x="-109.888" y="179.292" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R112" circuitNumber="1" x="-219.548" y="73.079" z="0.000" rz="0.00"/>
		<Component partNumber="510-500881" refDes="R112" circuitNumber="2" x="-83.408" y="73.079" z="0.000" rz="0.00"/>
		<Component partNumber="510-500881" refDes="R112" circuitNumber="3" x="-219.548" y="177.879" z="0.000" rz="0.00"/>
		<Component partNumber="510-500881" refDes="R112" circuitNumber="4" x="-83.408" y="177.879" z="0.000" rz="0.00"/>
		<Component partNumber="510-500881" refDes="R113" circuitNumber="1" x="-235.688" y="71.559" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R113" circuitNumber="2" x="-99.548" y="71.559" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R113" circuitNumber="3" x="-235.688" y="176.359" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R113" circuitNumber="4" x="-99.548" y="176.359" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R120" circuitNumber="1" x="-246.028" y="62.174" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R120" circuitNumber="2" x="-109.888" y="62.174" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R120" circuitNumber="3" x="-246.028" y="166.974" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R120" circuitNumber="4" x="-109.888" y="166.974" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R146" circuitNumber="1" x="-241.456" y="51.379" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R146" circuitNumber="2" x="-105.316" y="51.379" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R146" circuitNumber="3" x="-241.456" y="156.179" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R146" circuitNumber="4" x="-105.316" y="156.179" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R69" circuitNumber="1" x="-246.028" y="85.791" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R69" circuitNumber="2" x="-109.888" y="85.791" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R69" circuitNumber="3" x="-246.028" y="190.591" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R69" circuitNumber="4" x="-109.888" y="190.591" z="0.000" rz="90.00"/>
		<Component partNumber="510-500881" refDes="R91" circuitNumber="1" x="-237.956" y="78.278" z="0.000" rz="270.00"/>
		<Component partNumber="510-500881" refDes="R91" circuitNumber="2" x="-101.816" y="78.278" z="0.000" rz="270.00"/>
		<Component partNumber="510-500881" refDes="R91" circuitNumber="3" x="-237.956" y="183.078" z="0.000" rz="270.00"/>
		<Component partNumber="510-500881" refDes="R91" circuitNumber="4" x="-101.816" y="183.078" z="0.000" rz="270.00"/>
		<Component partNumber="510-500946" refDes="R202" circuitNumber="1" x="-241.158" y="15.459" z="0.000" rz="180.00"/>
		<Component partNumber="510-500946" refDes="R202" circuitNumber="2" x="-105.018" y="15.459" z="0.000" rz="180.00"/>
		<Component partNumber="510-500946" refDes="R202" circuitNumber="3" x="-241.158" y="120.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-500946" refDes="R202" circuitNumber="4" x="-105.018" y="120.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-500951" refDes="R204" circuitNumber="1" x="-240.988" y="13.839" z="0.000" rz="270.00"/>
		<Component partNumber="510-500951" refDes="R204" circuitNumber="2" x="-104.848" y="13.839" z="0.000" rz="270.00"/>
		<Component partNumber="510-500951" refDes="R204" circuitNumber="3" x="-240.988" y="118.639" z="0.000" rz="270.00"/>
		<Component partNumber="510-500951" refDes="R204" circuitNumber="4" x="-104.848" y="118.639" z="0.000" rz="270.00"/>
		<Component partNumber="510-500956" refDes="R115" circuitNumber="1" x="-153.338" y="66.989" z="0.000" rz="270.00"/>
		<Component partNumber="510-500956" refDes="R115" circuitNumber="2" x="-17.198" y="66.989" z="0.000" rz="270.00"/>
		<Component partNumber="510-500956" refDes="R115" circuitNumber="3" x="-153.338" y="171.789" z="0.000" rz="270.00"/>
		<Component partNumber="510-500956" refDes="R115" circuitNumber="4" x="-17.198" y="171.789" z="0.000" rz="270.00"/>
		<Component partNumber="510-500956" refDes="R116" circuitNumber="1" x="-152.338" y="66.979" z="0.000" rz="90.00"/>
		<Component partNumber="510-500956" refDes="R116" circuitNumber="2" x="-16.198" y="66.979" z="0.000" rz="90.00"/>
		<Component partNumber="510-500956" refDes="R116" circuitNumber="3" x="-152.338" y="171.779" z="0.000" rz="90.00"/>
		<Component partNumber="510-500956" refDes="R116" circuitNumber="4" x="-16.198" y="171.779" z="0.000" rz="90.00"/>
		<Component partNumber="510-500956" refDes="R128" circuitNumber="1" x="-187.208" y="58.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R128" circuitNumber="2" x="-51.068" y="58.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R128" circuitNumber="3" x="-187.208" y="163.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R128" circuitNumber="4" x="-51.068" y="163.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R129" circuitNumber="1" x="-168.158" y="58.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R129" circuitNumber="2" x="-32.018" y="58.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R129" circuitNumber="3" x="-168.158" y="163.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R129" circuitNumber="4" x="-32.018" y="163.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R138" circuitNumber="1" x="-187.218" y="57.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R138" circuitNumber="2" x="-51.078" y="57.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R138" circuitNumber="3" x="-187.218" y="162.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R138" circuitNumber="4" x="-51.078" y="162.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R139" circuitNumber="1" x="-168.168" y="57.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R139" circuitNumber="2" x="-32.028" y="57.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R139" circuitNumber="3" x="-168.168" y="162.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R139" circuitNumber="4" x="-32.028" y="162.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R147" circuitNumber="1" x="-226.638" y="49.729" z="0.000" rz="90.00"/>
		<Component partNumber="510-500956" refDes="R147" circuitNumber="2" x="-90.498" y="49.729" z="0.000" rz="90.00"/>
		<Component partNumber="510-500956" refDes="R147" circuitNumber="3" x="-226.638" y="154.529" z="0.000" rz="90.00"/>
		<Component partNumber="510-500956" refDes="R147" circuitNumber="4" x="-90.498" y="154.529" z="0.000" rz="90.00"/>
		<Component partNumber="510-500956" refDes="R148" circuitNumber="1" x="-225.638" y="49.739" z="0.000" rz="270.00"/>
		<Component partNumber="510-500956" refDes="R148" circuitNumber="2" x="-89.498" y="49.739" z="0.000" rz="270.00"/>
		<Component partNumber="510-500956" refDes="R148" circuitNumber="3" x="-225.638" y="154.539" z="0.000" rz="270.00"/>
		<Component partNumber="510-500956" refDes="R148" circuitNumber="4" x="-89.498" y="154.539" z="0.000" rz="270.00"/>
		<Component partNumber="510-500956" refDes="R189" circuitNumber="1" x="-187.208" y="27.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R189" circuitNumber="2" x="-51.068" y="27.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R189" circuitNumber="3" x="-187.208" y="132.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R189" circuitNumber="4" x="-51.068" y="132.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R190" circuitNumber="1" x="-168.158" y="27.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R190" circuitNumber="2" x="-32.018" y="27.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R190" circuitNumber="3" x="-168.158" y="132.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R190" circuitNumber="4" x="-32.018" y="132.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R196" circuitNumber="1" x="-187.218" y="26.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R196" circuitNumber="2" x="-51.078" y="26.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R196" circuitNumber="3" x="-187.218" y="131.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R196" circuitNumber="4" x="-51.078" y="131.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R197" circuitNumber="1" x="-168.168" y="26.259" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R197" circuitNumber="2" x="-32.028" y="26.259" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R197" circuitNumber="3" x="-168.168" y="131.059" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R197" circuitNumber="4" x="-32.028" y="131.059" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R53" circuitNumber="1" x="-187.208" y="89.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R53" circuitNumber="2" x="-51.068" y="89.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R53" circuitNumber="3" x="-187.208" y="194.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R53" circuitNumber="4" x="-51.068" y="194.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R54" circuitNumber="1" x="-168.158" y="89.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R54" circuitNumber="2" x="-32.018" y="89.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R54" circuitNumber="3" x="-168.158" y="194.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R54" circuitNumber="4" x="-32.018" y="194.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R62" circuitNumber="1" x="-187.218" y="88.309" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R62" circuitNumber="2" x="-51.078" y="88.309" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R62" circuitNumber="3" x="-187.218" y="193.109" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R62" circuitNumber="4" x="-51.078" y="193.109" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R63" circuitNumber="1" x="-168.168" y="88.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R63" circuitNumber="2" x="-32.028" y="88.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R63" circuitNumber="3" x="-168.168" y="193.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R63" circuitNumber="4" x="-32.028" y="193.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R96" circuitNumber="1" x="-221.468" y="77.139" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R96" circuitNumber="2" x="-85.328" y="77.139" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R96" circuitNumber="3" x="-221.468" y="181.939" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R96" circuitNumber="4" x="-85.328" y="181.939" z="0.000" rz="0.00"/>
		<Component partNumber="510-500956" refDes="R99" circuitNumber="1" x="-221.458" y="76.149" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R99" circuitNumber="2" x="-85.318" y="76.149" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R99" circuitNumber="3" x="-221.458" y="180.949" z="0.000" rz="180.00"/>
		<Component partNumber="510-500956" refDes="R99" circuitNumber="4" x="-85.318" y="180.949" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R149" circuitNumber="1" x="-249.330" y="48.838" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R149" circuitNumber="2" x="-113.190" y="48.838" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R149" circuitNumber="3" x="-249.330" y="153.638" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R149" circuitNumber="4" x="-113.190" y="153.638" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R170" circuitNumber="1" x="-227.105" y="34.487" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R170" circuitNumber="2" x="-90.965" y="34.487" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R170" circuitNumber="3" x="-227.105" y="139.287" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R170" circuitNumber="4" x="-90.965" y="139.287" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R177" circuitNumber="1" x="-231.042" y="32.963" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R177" circuitNumber="2" x="-94.902" y="32.963" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R177" circuitNumber="3" x="-231.042" y="137.763" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R177" circuitNumber="4" x="-94.902" y="137.763" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R25" circuitNumber="1" x="-229.645" y="101.729" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R25" circuitNumber="2" x="-93.505" y="101.729" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R25" circuitNumber="3" x="-229.645" y="206.529" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R25" circuitNumber="4" x="-93.505" y="206.529" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R34" circuitNumber="1" x="-231.169" y="101.729" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R34" circuitNumber="2" x="-95.029" y="101.729" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R34" circuitNumber="3" x="-231.169" y="206.529" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R34" circuitNumber="4" x="-95.029" y="206.529" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R349" circuitNumber="1" x="-215.188" y="30.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R349" circuitNumber="2" x="-79.048" y="30.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R349" circuitNumber="3" x="-215.188" y="135.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R349" circuitNumber="4" x="-79.048" y="135.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-500980" refDes="R366" circuitNumber="1" x="-227.388" y="102.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R366" circuitNumber="2" x="-91.248" y="102.359" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R366" circuitNumber="3" x="-227.388" y="207.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R366" circuitNumber="4" x="-91.248" y="207.159" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R367" circuitNumber="1" x="-227.388" y="100.759" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R367" circuitNumber="2" x="-91.248" y="100.759" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R367" circuitNumber="3" x="-227.388" y="205.559" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R367" circuitNumber="4" x="-91.248" y="205.559" z="0.000" rz="0.00"/>
		<Component partNumber="510-500980" refDes="R368" circuitNumber="1" x="-226.138" y="104.259" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R368" circuitNumber="2" x="-89.998" y="104.259" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R368" circuitNumber="3" x="-226.138" y="209.059" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R368" circuitNumber="4" x="-89.998" y="209.059" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R44" circuitNumber="1" x="-253.521" y="101.670" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R44" circuitNumber="2" x="-117.381" y="101.670" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R44" circuitNumber="3" x="-253.521" y="206.470" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R44" circuitNumber="4" x="-117.381" y="206.470" z="0.000" rz="90.00"/>
		<Component partNumber="510-500980" refDes="R60" circuitNumber="1" x="-259.363" y="88.716" z="0.000" rz="270.00"/>
		<Component partNumber="510-500980" refDes="R60" circuitNumber="2" x="-123.223" y="88.716" z="0.000" rz="270.00"/>
		<Component partNumber="510-500980" refDes="R60" circuitNumber="3" x="-259.363" y="193.516" z="0.000" rz="270.00"/>
		<Component partNumber="510-500980" refDes="R60" circuitNumber="4" x="-123.223" y="193.516" z="0.000" rz="270.00"/>
		<Component partNumber="510-500980" refDes="R66" circuitNumber="1" x="-216.738" y="33.609" z="0.000" rz="270.00"/>
		<Component partNumber="510-500980" refDes="R66" circuitNumber="2" x="-80.598" y="33.609" z="0.000" rz="270.00"/>
		<Component partNumber="510-500980" refDes="R66" circuitNumber="3" x="-216.738" y="138.409" z="0.000" rz="270.00"/>
		<Component partNumber="510-500980" refDes="R66" circuitNumber="4" x="-80.598" y="138.409" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R27" circuitNumber="1" x="-228.629" y="102.998" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R27" circuitNumber="2" x="-92.489" y="102.998" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R27" circuitNumber="3" x="-228.629" y="207.798" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R27" circuitNumber="4" x="-92.489" y="207.798" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R32" circuitNumber="1" x="-234.090" y="102.921" z="0.000" rz="0.00"/>
		<Component partNumber="510-500983" refDes="R32" circuitNumber="2" x="-97.950" y="102.921" z="0.000" rz="0.00"/>
		<Component partNumber="510-500983" refDes="R32" circuitNumber="3" x="-234.090" y="207.721" z="0.000" rz="0.00"/>
		<Component partNumber="510-500983" refDes="R32" circuitNumber="4" x="-97.950" y="207.721" z="0.000" rz="0.00"/>
		<Component partNumber="510-500983" refDes="R40" circuitNumber="1" x="-228.629" y="101.728" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R40" circuitNumber="2" x="-92.489" y="101.728" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R40" circuitNumber="3" x="-228.629" y="206.528" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R40" circuitNumber="4" x="-92.489" y="206.528" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R5" circuitNumber="1" x="-228.629" y="104.268" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R5" circuitNumber="2" x="-92.489" y="104.268" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R5" circuitNumber="3" x="-228.629" y="209.068" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R5" circuitNumber="4" x="-92.489" y="209.068" z="0.000" rz="270.00"/>
		<Component partNumber="510-500983" refDes="R93" circuitNumber="1" x="-264.316" y="79.375" z="0.000" rz="0.00"/>
		<Component partNumber="510-500983" refDes="R93" circuitNumber="2" x="-128.176" y="79.375" z="0.000" rz="0.00"/>
		<Component partNumber="510-500983" refDes="R93" circuitNumber="3" x="-264.316" y="184.175" z="0.000" rz="0.00"/>
		<Component partNumber="510-500983" refDes="R93" circuitNumber="4" x="-128.176" y="184.175" z="0.000" rz="0.00"/>
		<Component partNumber="510-500989" refDes="R20" circuitNumber="1" x="-234.090" y="103.683" z="0.000" rz="180.00"/>
		<Component partNumber="510-500989" refDes="R20" circuitNumber="2" x="-97.950" y="103.683" z="0.000" rz="180.00"/>
		<Component partNumber="510-500989" refDes="R20" circuitNumber="3" x="-234.090" y="208.483" z="0.000" rz="180.00"/>
		<Component partNumber="510-500989" refDes="R20" circuitNumber="4" x="-97.950" y="208.483" z="0.000" rz="180.00"/>
		<Component partNumber="510-501007" refDes="R187" circuitNumber="1" x="-232.488" y="26.909" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R187" circuitNumber="2" x="-96.348" y="26.909" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R187" circuitNumber="3" x="-232.488" y="131.709" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R187" circuitNumber="4" x="-96.348" y="131.709" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R198" circuitNumber="1" x="-223.676" y="24.454" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R198" circuitNumber="2" x="-87.536" y="24.454" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R198" circuitNumber="3" x="-223.676" y="129.254" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R198" circuitNumber="4" x="-87.536" y="129.254" z="0.000" rz="270.00"/>
		<Component partNumber="510-501007" refDes="R199" circuitNumber="1" x="-213.039" y="25.859" z="0.000" rz="180.00"/>
		<Component partNumber="510-501007" refDes="R199" circuitNumber="2" x="-76.899" y="25.859" z="0.000" rz="180.00"/>
		<Component partNumber="510-501007" refDes="R199" circuitNumber="3" x="-213.039" y="130.659" z="0.000" rz="180.00"/>
		<Component partNumber="510-501007" refDes="R199" circuitNumber="4" x="-76.899" y="130.659" z="0.000" rz="180.00"/>
		<Component partNumber="510-501007" refDes="R292" circuitNumber="1" x="-254.638" y="18.859" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R292" circuitNumber="2" x="-118.498" y="18.859" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R292" circuitNumber="3" x="-254.638" y="123.659" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R292" circuitNumber="4" x="-118.498" y="123.659" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R48" circuitNumber="1" x="-257.538" y="97.959" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R48" circuitNumber="2" x="-121.398" y="97.959" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R48" circuitNumber="3" x="-257.538" y="202.759" z="0.000" rz="90.00"/>
		<Component partNumber="510-501007" refDes="R48" circuitNumber="4" x="-121.398" y="202.759" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R108" circuitNumber="1" x="-183.139" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R108" circuitNumber="2" x="-46.999" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R108" circuitNumber="3" x="-183.139" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R108" circuitNumber="4" x="-46.999" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R109" circuitNumber="1" x="-181.615" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R109" circuitNumber="2" x="-45.475" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R109" circuitNumber="3" x="-181.615" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R109" circuitNumber="4" x="-45.475" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R110" circuitNumber="1" x="-164.089" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R110" circuitNumber="2" x="-27.949" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R110" circuitNumber="3" x="-164.089" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R110" circuitNumber="4" x="-27.949" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R111" circuitNumber="1" x="-162.565" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R111" circuitNumber="2" x="-26.425" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R111" circuitNumber="3" x="-162.565" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R111" circuitNumber="4" x="-26.425" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R122" circuitNumber="1" x="-138.054" y="60.649" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R122" circuitNumber="2" x="-1.914" y="60.649" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R122" circuitNumber="3" x="-138.054" y="165.449" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R122" circuitNumber="4" x="-1.914" y="165.449" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R126" circuitNumber="1" x="-153.040" y="59.126" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R126" circuitNumber="2" x="-16.900" y="59.126" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R126" circuitNumber="3" x="-153.040" y="163.926" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R126" circuitNumber="4" x="-16.900" y="163.926" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R134" circuitNumber="1" x="-183.139" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R134" circuitNumber="2" x="-46.999" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R134" circuitNumber="3" x="-183.139" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R134" circuitNumber="4" x="-46.999" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R135" circuitNumber="1" x="-181.615" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R135" circuitNumber="2" x="-45.475" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R135" circuitNumber="3" x="-181.615" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R135" circuitNumber="4" x="-45.475" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R136" circuitNumber="1" x="-164.089" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R136" circuitNumber="2" x="-27.949" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R136" circuitNumber="3" x="-164.089" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R136" circuitNumber="4" x="-27.949" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R137" circuitNumber="1" x="-162.565" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R137" circuitNumber="2" x="-26.425" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R137" circuitNumber="3" x="-162.565" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R137" circuitNumber="4" x="-26.425" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R151" circuitNumber="1" x="-210.595" y="45.663" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R151" circuitNumber="2" x="-74.455" y="45.663" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R151" circuitNumber="3" x="-210.595" y="150.463" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R151" circuitNumber="4" x="-74.455" y="150.463" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R155" circuitNumber="1" x="-226.089" y="44.140" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R155" circuitNumber="2" x="-89.949" y="44.140" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R155" circuitNumber="3" x="-226.089" y="148.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R155" circuitNumber="4" x="-89.949" y="148.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R160" circuitNumber="1" x="-183.139" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R160" circuitNumber="2" x="-46.999" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R160" circuitNumber="3" x="-183.139" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R160" circuitNumber="4" x="-46.999" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R161" circuitNumber="1" x="-181.615" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R161" circuitNumber="2" x="-45.475" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R161" circuitNumber="3" x="-181.615" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R161" circuitNumber="4" x="-45.475" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R162" circuitNumber="1" x="-164.089" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R162" circuitNumber="2" x="-27.949" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R162" circuitNumber="3" x="-164.089" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R162" circuitNumber="4" x="-27.949" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R163" circuitNumber="1" x="-162.565" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R163" circuitNumber="2" x="-26.425" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R163" circuitNumber="3" x="-162.565" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R163" circuitNumber="4" x="-26.425" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R191" circuitNumber="1" x="-183.139" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R191" circuitNumber="2" x="-46.999" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R191" circuitNumber="3" x="-183.139" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R191" circuitNumber="4" x="-46.999" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R192" circuitNumber="1" x="-181.615" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R192" circuitNumber="2" x="-45.475" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R192" circuitNumber="3" x="-181.615" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R192" circuitNumber="4" x="-45.475" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R193" circuitNumber="1" x="-164.089" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R193" circuitNumber="2" x="-27.949" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R193" circuitNumber="3" x="-164.089" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R193" circuitNumber="4" x="-27.949" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R194" circuitNumber="1" x="-162.565" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R194" circuitNumber="2" x="-26.425" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R194" circuitNumber="3" x="-162.565" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R194" circuitNumber="4" x="-26.425" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R207" circuitNumber="1" x="-181.615" y="11.984" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R207" circuitNumber="2" x="-45.475" y="11.984" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R207" circuitNumber="3" x="-181.615" y="116.784" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R207" circuitNumber="4" x="-45.475" y="116.784" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R208" circuitNumber="1" x="-162.565" y="11.984" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R208" circuitNumber="2" x="-26.425" y="11.984" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R208" circuitNumber="3" x="-162.565" y="116.784" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R208" circuitNumber="4" x="-26.425" y="116.784" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R22" circuitNumber="1" x="-183.139" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R22" circuitNumber="2" x="-46.999" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R22" circuitNumber="3" x="-183.139" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R22" circuitNumber="4" x="-46.999" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R23" circuitNumber="1" x="-164.089" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R23" circuitNumber="2" x="-27.949" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R23" circuitNumber="3" x="-164.089" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R23" circuitNumber="4" x="-27.949" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R249" circuitNumber="1" x="-226.089" y="45.663" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R249" circuitNumber="2" x="-89.949" y="45.663" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R249" circuitNumber="3" x="-226.089" y="150.463" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R249" circuitNumber="4" x="-89.949" y="150.463" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R254" circuitNumber="1" x="-241.456" y="44.140" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R254" circuitNumber="2" x="-105.316" y="44.140" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R254" circuitNumber="3" x="-241.456" y="148.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R254" circuitNumber="4" x="-105.316" y="148.940" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R56" circuitNumber="1" x="-183.139" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R56" circuitNumber="2" x="-46.999" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R56" circuitNumber="3" x="-183.139" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R56" circuitNumber="4" x="-46.999" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R57" circuitNumber="1" x="-181.615" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R57" circuitNumber="2" x="-45.475" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R57" circuitNumber="3" x="-181.615" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R57" circuitNumber="4" x="-45.475" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R58" circuitNumber="1" x="-164.089" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R58" circuitNumber="2" x="-27.949" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R58" circuitNumber="3" x="-164.089" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R58" circuitNumber="4" x="-27.949" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="510-501070" refDes="R59" circuitNumber="1" x="-162.565" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R59" circuitNumber="2" x="-26.425" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R59" circuitNumber="3" x="-162.565" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R59" circuitNumber="4" x="-26.425" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="510-501070" refDes="R87" circuitNumber="1" x="-138.054" y="80.949" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R87" circuitNumber="2" x="-1.914" y="80.949" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R87" circuitNumber="3" x="-138.054" y="185.749" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R87" circuitNumber="4" x="-1.914" y="185.749" z="0.000" rz="270.00"/>
		<Component partNumber="510-501070" refDes="R94" circuitNumber="1" x="-153.040" y="78.876" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R94" circuitNumber="2" x="-16.900" y="78.876" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R94" circuitNumber="3" x="-153.040" y="183.676" z="0.000" rz="90.00"/>
		<Component partNumber="510-501070" refDes="R94" circuitNumber="4" x="-16.900" y="183.676" z="0.000" rz="90.00"/>
		<Component partNumber="510-501108" refDes="R142" circuitNumber="1" x="-255.538" y="58.239" z="0.000" rz="270.00"/>
		<Component partNumber="510-501108" refDes="R142" circuitNumber="2" x="-119.398" y="58.239" z="0.000" rz="270.00"/>
		<Component partNumber="510-501108" refDes="R142" circuitNumber="3" x="-255.538" y="163.039" z="0.000" rz="270.00"/>
		<Component partNumber="510-501108" refDes="R142" circuitNumber="4" x="-119.398" y="163.039" z="0.000" rz="270.00"/>
		<Component partNumber="510-501118" refDes="R49" circuitNumber="1" x="-214.024" y="93.669" z="0.000" rz="180.00"/>
		<Component partNumber="510-501118" refDes="R49" circuitNumber="2" x="-77.884" y="93.669" z="0.000" rz="180.00"/>
		<Component partNumber="510-501118" refDes="R49" circuitNumber="3" x="-214.024" y="198.469" z="0.000" rz="180.00"/>
		<Component partNumber="510-501118" refDes="R49" circuitNumber="4" x="-77.884" y="198.469" z="0.000" rz="180.00"/>
		<Component partNumber="510-501177" refDes="R102" circuitNumber="1" x="-268.026" y="77.462" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R102" circuitNumber="2" x="-131.886" y="77.462" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R102" circuitNumber="3" x="-268.026" y="182.262" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R102" circuitNumber="4" x="-131.886" y="182.262" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R17" circuitNumber="1" x="-242.599" y="103.683" z="0.000" rz="180.00"/>
		<Component partNumber="510-501177" refDes="R17" circuitNumber="2" x="-106.459" y="103.683" z="0.000" rz="180.00"/>
		<Component partNumber="510-501177" refDes="R17" circuitNumber="3" x="-242.599" y="208.483" z="0.000" rz="180.00"/>
		<Component partNumber="510-501177" refDes="R17" circuitNumber="4" x="-106.459" y="208.483" z="0.000" rz="180.00"/>
		<Component partNumber="510-501177" refDes="R19" circuitNumber="1" x="-237.646" y="103.683" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R19" circuitNumber="2" x="-101.506" y="103.683" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R19" circuitNumber="3" x="-237.646" y="208.483" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R19" circuitNumber="4" x="-101.506" y="208.483" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R427" circuitNumber="1" x="-236.688" y="14.959" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R427" circuitNumber="2" x="-100.548" y="14.959" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R427" circuitNumber="3" x="-236.688" y="119.759" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R427" circuitNumber="4" x="-100.548" y="119.759" z="0.000" rz="90.00"/>
		<Component partNumber="510-501177" refDes="R428" circuitNumber="1" x="-247.388" y="101.809" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R428" circuitNumber="2" x="-111.248" y="101.809" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R428" circuitNumber="3" x="-247.388" y="206.609" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R428" circuitNumber="4" x="-111.248" y="206.609" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R429" circuitNumber="1" x="-246.538" y="101.559" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R429" circuitNumber="2" x="-110.398" y="101.559" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R429" circuitNumber="3" x="-246.538" y="206.359" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R429" circuitNumber="4" x="-110.398" y="206.359" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R45" circuitNumber="1" x="-257.538" y="100.458" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R45" circuitNumber="2" x="-121.398" y="100.458" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R45" circuitNumber="3" x="-257.538" y="205.258" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R45" circuitNumber="4" x="-121.398" y="205.258" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R46" circuitNumber="1" x="-257.538" y="99.358" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R46" circuitNumber="2" x="-121.398" y="99.358" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R46" circuitNumber="3" x="-257.538" y="204.158" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R46" circuitNumber="4" x="-121.398" y="204.158" z="0.000" rz="270.00"/>
		<Component partNumber="510-501177" refDes="R76" circuitNumber="1" x="-264.316" y="83.312" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R76" circuitNumber="2" x="-128.176" y="83.312" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R76" circuitNumber="3" x="-264.316" y="188.112" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R76" circuitNumber="4" x="-128.176" y="188.112" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R86" circuitNumber="1" x="-264.316" y="80.899" z="0.000" rz="180.00"/>
		<Component partNumber="510-501177" refDes="R86" circuitNumber="2" x="-128.176" y="80.899" z="0.000" rz="180.00"/>
		<Component partNumber="510-501177" refDes="R86" circuitNumber="3" x="-264.316" y="185.699" z="0.000" rz="180.00"/>
		<Component partNumber="510-501177" refDes="R86" circuitNumber="4" x="-128.176" y="185.699" z="0.000" rz="180.00"/>
		<Component partNumber="510-501177" refDes="R88" circuitNumber="1" x="-265.788" y="81.709" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R88" circuitNumber="2" x="-129.648" y="81.709" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R88" circuitNumber="3" x="-265.788" y="186.509" z="0.000" rz="0.00"/>
		<Component partNumber="510-501177" refDes="R88" circuitNumber="4" x="-129.648" y="186.509" z="0.000" rz="0.00"/>
		<Component partNumber="510-501194" refDes="R201" circuitNumber="1" x="-222.787" y="16.118" z="0.000" rz="0.00"/>
		<Component partNumber="510-501194" refDes="R201" circuitNumber="2" x="-86.647" y="16.118" z="0.000" rz="0.00"/>
		<Component partNumber="510-501194" refDes="R201" circuitNumber="3" x="-222.787" y="120.918" z="0.000" rz="0.00"/>
		<Component partNumber="510-501194" refDes="R201" circuitNumber="4" x="-86.647" y="120.918" z="0.000" rz="0.00"/>
		<Component partNumber="510-501320" refDes="R89" circuitNumber="1" x="-262.758" y="80.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-501320" refDes="R89" circuitNumber="2" x="-126.618" y="80.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-501320" refDes="R89" circuitNumber="3" x="-262.758" y="184.909" z="0.000" rz="180.00"/>
		<Component partNumber="510-501320" refDes="R89" circuitNumber="4" x="-126.618" y="184.909" z="0.000" rz="180.00"/>
		<Component partNumber="510-501443" refDes="R238" circuitNumber="1" x="-267.669" y="69.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-501443" refDes="R238" circuitNumber="2" x="-131.529" y="69.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-501443" refDes="R238" circuitNumber="3" x="-267.669" y="174.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-501443" refDes="R238" circuitNumber="4" x="-131.529" y="174.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-501547" refDes="R288" circuitNumber="1" x="-223.088" y="21.679" z="0.000" rz="90.00"/>
		<Component partNumber="510-501547" refDes="R288" circuitNumber="2" x="-86.948" y="21.679" z="0.000" rz="90.00"/>
		<Component partNumber="510-501547" refDes="R288" circuitNumber="3" x="-223.088" y="126.479" z="0.000" rz="90.00"/>
		<Component partNumber="510-501547" refDes="R288" circuitNumber="4" x="-86.948" y="126.479" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R130" circuitNumber="1" x="-239.043" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R130" circuitNumber="2" x="-102.903" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R130" circuitNumber="3" x="-239.043" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R130" circuitNumber="4" x="-102.903" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R15" circuitNumber="1" x="-245.139" y="102.559" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R15" circuitNumber="2" x="-108.999" y="102.559" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R15" circuitNumber="3" x="-245.139" y="207.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R15" circuitNumber="4" x="-108.999" y="207.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R152" circuitNumber="1" x="-245.901" y="43.747" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R152" circuitNumber="2" x="-109.761" y="43.747" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R152" circuitNumber="3" x="-245.901" y="148.547" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R152" circuitNumber="4" x="-109.761" y="148.547" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R153" circuitNumber="1" x="-245.266" y="43.747" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R153" circuitNumber="2" x="-109.126" y="43.747" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R153" circuitNumber="3" x="-245.266" y="148.547" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R153" circuitNumber="4" x="-109.126" y="148.547" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R159" circuitNumber="1" x="-247.425" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R159" circuitNumber="2" x="-111.285" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R159" circuitNumber="3" x="-247.425" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R159" circuitNumber="4" x="-111.285" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R16" circuitNumber="1" x="-247.425" y="102.559" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R16" circuitNumber="2" x="-111.285" y="102.559" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R16" circuitNumber="3" x="-247.425" y="207.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R16" circuitNumber="4" x="-111.285" y="207.359" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R29" circuitNumber="1" x="-247.425" y="103.321" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R29" circuitNumber="2" x="-111.285" y="103.321" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R29" circuitNumber="3" x="-247.425" y="208.121" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R29" circuitNumber="4" x="-111.285" y="208.121" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R30" circuitNumber="1" x="-245.139" y="104.083" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R30" circuitNumber="2" x="-108.999" y="104.083" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R30" circuitNumber="3" x="-245.139" y="208.883" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R30" circuitNumber="4" x="-108.999" y="208.883" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R314" circuitNumber="1" x="-236.503" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R314" circuitNumber="2" x="-100.363" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R314" circuitNumber="3" x="-236.503" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R314" circuitNumber="4" x="-100.363" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R315" circuitNumber="1" x="-237.138" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R315" circuitNumber="2" x="-100.998" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R315" circuitNumber="3" x="-237.138" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R315" circuitNumber="4" x="-100.998" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R316" circuitNumber="1" x="-237.773" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R316" circuitNumber="2" x="-101.633" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R316" circuitNumber="3" x="-237.773" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R316" circuitNumber="4" x="-101.633" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R317" circuitNumber="1" x="-238.408" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R317" circuitNumber="2" x="-102.268" y="58.110" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R317" circuitNumber="3" x="-238.408" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R317" circuitNumber="4" x="-102.268" y="162.910" z="0.000" rz="90.00"/>
		<Component partNumber="510-501668" refDes="R338" circuitNumber="1" x="-245.266" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R338" circuitNumber="2" x="-109.126" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R338" circuitNumber="3" x="-245.266" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R338" circuitNumber="4" x="-109.126" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R339" circuitNumber="1" x="-245.901" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R339" circuitNumber="2" x="-109.761" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R339" circuitNumber="3" x="-245.901" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R339" circuitNumber="4" x="-109.761" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R340" circuitNumber="1" x="-244.631" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R340" circuitNumber="2" x="-108.491" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R340" circuitNumber="3" x="-244.631" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R340" circuitNumber="4" x="-108.491" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R341" circuitNumber="1" x="-243.996" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R341" circuitNumber="2" x="-107.856" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R341" circuitNumber="3" x="-243.996" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R341" circuitNumber="4" x="-107.856" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R345" circuitNumber="1" x="-246.790" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R345" circuitNumber="2" x="-110.650" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R345" circuitNumber="3" x="-246.790" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R345" circuitNumber="4" x="-110.650" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-501668" refDes="R36" circuitNumber="1" x="-245.139" y="103.321" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R36" circuitNumber="2" x="-108.999" y="103.321" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R36" circuitNumber="3" x="-245.139" y="208.121" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R36" circuitNumber="4" x="-108.999" y="208.121" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R37" circuitNumber="1" x="-247.425" y="104.083" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R37" circuitNumber="2" x="-111.285" y="104.083" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R37" circuitNumber="3" x="-247.425" y="208.883" z="0.000" rz="180.00"/>
		<Component partNumber="510-501668" refDes="R37" circuitNumber="4" x="-111.285" y="208.883" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R105" circuitNumber="1" x="-262.792" y="76.146" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R105" circuitNumber="2" x="-126.652" y="76.146" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R105" circuitNumber="3" x="-262.792" y="180.946" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R105" circuitNumber="4" x="-126.652" y="180.946" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R121" circuitNumber="1" x="-244.627" y="64.205" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R121" circuitNumber="2" x="-108.487" y="64.205" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R121" circuitNumber="3" x="-244.627" y="169.005" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R121" circuitNumber="4" x="-108.487" y="169.005" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R13" circuitNumber="1" x="-262.080" y="104.264" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R13" circuitNumber="2" x="-125.940" y="104.264" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R13" circuitNumber="3" x="-262.080" y="209.064" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R13" circuitNumber="4" x="-125.940" y="209.064" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R132" circuitNumber="1" x="-260.171" y="59.355" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R132" circuitNumber="2" x="-124.031" y="59.355" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R132" circuitNumber="3" x="-260.171" y="164.155" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R132" circuitNumber="4" x="-124.031" y="164.155" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R168" circuitNumber="1" x="-239.678" y="34.487" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R168" circuitNumber="2" x="-103.538" y="34.487" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R168" circuitNumber="3" x="-239.678" y="139.287" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R168" circuitNumber="4" x="-103.538" y="139.287" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R169" circuitNumber="1" x="-231.042" y="34.487" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R169" circuitNumber="2" x="-94.902" y="34.487" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R169" circuitNumber="3" x="-231.042" y="139.287" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R169" circuitNumber="4" x="-94.902" y="139.287" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R171" circuitNumber="1" x="-223.295" y="34.487" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R171" circuitNumber="2" x="-87.155" y="34.487" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R171" circuitNumber="3" x="-223.295" y="139.287" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R171" circuitNumber="4" x="-87.155" y="139.287" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R172" circuitNumber="1" x="-227.105" y="33.725" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R172" circuitNumber="2" x="-90.965" y="33.725" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R172" circuitNumber="3" x="-227.105" y="138.525" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R172" circuitNumber="4" x="-90.965" y="138.525" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R173" circuitNumber="1" x="-223.295" y="33.725" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R173" circuitNumber="2" x="-87.155" y="33.725" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R173" circuitNumber="3" x="-223.295" y="138.525" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R173" circuitNumber="4" x="-87.155" y="138.525" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R178" circuitNumber="1" x="-227.105" y="32.963" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R178" circuitNumber="2" x="-90.965" y="32.963" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R178" circuitNumber="3" x="-227.105" y="137.763" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R178" circuitNumber="4" x="-90.965" y="137.763" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R179" circuitNumber="1" x="-223.295" y="32.963" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R179" circuitNumber="2" x="-87.155" y="32.963" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R179" circuitNumber="3" x="-223.295" y="137.763" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R179" circuitNumber="4" x="-87.155" y="137.763" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R183" circuitNumber="1" x="-227.105" y="32.201" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R183" circuitNumber="2" x="-90.965" y="32.201" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R183" circuitNumber="3" x="-227.105" y="137.001" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R183" circuitNumber="4" x="-90.965" y="137.001" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R186" circuitNumber="1" x="-223.195" y="30.677" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R186" circuitNumber="2" x="-87.055" y="30.677" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R186" circuitNumber="3" x="-223.195" y="135.477" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R186" circuitNumber="4" x="-87.055" y="135.477" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R348" circuitNumber="1" x="-229.388" y="100.859" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R348" circuitNumber="2" x="-93.248" y="100.859" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R348" circuitNumber="3" x="-229.388" y="205.659" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R348" circuitNumber="4" x="-93.248" y="205.659" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R350" circuitNumber="1" x="-213.138" y="29.009" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R350" circuitNumber="2" x="-76.998" y="29.009" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R350" circuitNumber="3" x="-213.138" y="133.809" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R350" circuitNumber="4" x="-76.998" y="133.809" z="0.000" rz="90.00"/>
		<Component partNumber="510-501765" refDes="R426" circuitNumber="1" x="-267.138" y="75.709" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R426" circuitNumber="2" x="-130.998" y="75.709" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R426" circuitNumber="3" x="-267.138" y="180.509" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R426" circuitNumber="4" x="-130.998" y="180.509" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R43" circuitNumber="1" x="-230.407" y="102.998" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R43" circuitNumber="2" x="-94.267" y="102.998" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R43" circuitNumber="3" x="-230.407" y="207.798" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R43" circuitNumber="4" x="-94.267" y="207.798" z="0.000" rz="270.00"/>
		<Component partNumber="510-501765" refDes="R71" circuitNumber="1" x="-223.538" y="85.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R71" circuitNumber="2" x="-87.398" y="85.309" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R71" circuitNumber="3" x="-223.538" y="190.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R71" circuitNumber="4" x="-87.398" y="190.109" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R78" circuitNumber="1" x="-262.665" y="82.550" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R78" circuitNumber="2" x="-126.525" y="82.550" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R78" circuitNumber="3" x="-262.665" y="187.350" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R78" circuitNumber="4" x="-126.525" y="187.350" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R84" circuitNumber="1" x="-264.316" y="81.661" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R84" circuitNumber="2" x="-128.176" y="81.661" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R84" circuitNumber="3" x="-264.316" y="186.461" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R84" circuitNumber="4" x="-128.176" y="186.461" z="0.000" rz="180.00"/>
		<Component partNumber="510-501765" refDes="R97" circuitNumber="1" x="-268.026" y="78.224" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R97" circuitNumber="2" x="-131.886" y="78.224" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R97" circuitNumber="3" x="-268.026" y="183.024" z="0.000" rz="0.00"/>
		<Component partNumber="510-501765" refDes="R97" circuitNumber="4" x="-131.886" y="183.024" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R103" circuitNumber="1" x="-264.316" y="76.962" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R103" circuitNumber="2" x="-128.176" y="76.962" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R103" circuitNumber="3" x="-264.316" y="181.762" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R103" circuitNumber="4" x="-128.176" y="181.762" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R118" circuitNumber="1" x="-268.038" y="68.459" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R118" circuitNumber="2" x="-131.898" y="68.459" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R118" circuitNumber="3" x="-268.038" y="173.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R118" circuitNumber="4" x="-131.898" y="173.259" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R28" circuitNumber="1" x="-220.628" y="102.998" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R28" circuitNumber="2" x="-84.488" y="102.998" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R28" circuitNumber="3" x="-220.628" y="207.798" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R28" circuitNumber="4" x="-84.488" y="207.798" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R309" circuitNumber="1" x="-266.856" y="81.711" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R309" circuitNumber="2" x="-130.716" y="81.711" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R309" circuitNumber="3" x="-266.856" y="186.511" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R309" circuitNumber="4" x="-130.716" y="186.511" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R310" circuitNumber="1" x="-267.688" y="70.709" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R310" circuitNumber="2" x="-131.548" y="70.709" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R310" circuitNumber="3" x="-267.688" y="175.509" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R310" circuitNumber="4" x="-131.548" y="175.509" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R312" circuitNumber="1" x="-267.688" y="71.908" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R312" circuitNumber="2" x="-131.548" y="71.908" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R312" circuitNumber="3" x="-267.688" y="176.708" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R312" circuitNumber="4" x="-131.548" y="176.708" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R33" circuitNumber="1" x="-232.693" y="102.921" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R33" circuitNumber="2" x="-96.553" y="102.921" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R33" circuitNumber="3" x="-232.693" y="207.721" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R33" circuitNumber="4" x="-96.553" y="207.721" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R346" circuitNumber="1" x="-217.958" y="104.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R346" circuitNumber="2" x="-81.818" y="104.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R346" circuitNumber="3" x="-217.958" y="208.959" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R346" circuitNumber="4" x="-81.818" y="208.959" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R347" circuitNumber="1" x="-223.288" y="100.659" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R347" circuitNumber="2" x="-87.148" y="100.659" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R347" circuitNumber="3" x="-223.288" y="205.459" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R347" circuitNumber="4" x="-87.148" y="205.459" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R359" circuitNumber="1" x="-214.338" y="29.009" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R359" circuitNumber="2" x="-78.198" y="29.009" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R359" circuitNumber="3" x="-214.338" y="133.809" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R359" circuitNumber="4" x="-78.198" y="133.809" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R364" circuitNumber="1" x="-215.438" y="25.859" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R364" circuitNumber="2" x="-79.298" y="25.859" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R364" circuitNumber="3" x="-215.438" y="130.659" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R364" circuitNumber="4" x="-79.298" y="130.659" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R38" circuitNumber="1" x="-232.693" y="102.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R38" circuitNumber="2" x="-96.553" y="102.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R38" circuitNumber="3" x="-232.693" y="206.959" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R38" circuitNumber="4" x="-96.553" y="206.959" z="0.000" rz="180.00"/>
		<Component partNumber="510-501843" refDes="R39" circuitNumber="1" x="-220.628" y="101.855" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R39" circuitNumber="2" x="-84.488" y="101.855" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R39" circuitNumber="3" x="-220.628" y="206.655" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R39" circuitNumber="4" x="-84.488" y="206.655" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R70" circuitNumber="1" x="-227.496" y="85.478" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R70" circuitNumber="2" x="-91.356" y="85.478" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R70" circuitNumber="3" x="-227.496" y="190.278" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R70" circuitNumber="4" x="-91.356" y="190.278" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R75" circuitNumber="1" x="-223.295" y="82.239" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R75" circuitNumber="2" x="-87.155" y="82.239" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R75" circuitNumber="3" x="-223.295" y="187.039" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R75" circuitNumber="4" x="-87.155" y="187.039" z="0.000" rz="270.00"/>
		<Component partNumber="510-501843" refDes="R81" circuitNumber="1" x="-262.665" y="81.788" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R81" circuitNumber="2" x="-126.525" y="81.788" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R81" circuitNumber="3" x="-262.665" y="186.588" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R81" circuitNumber="4" x="-126.525" y="186.588" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R92" circuitNumber="1" x="-267.772" y="80.006" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R92" circuitNumber="2" x="-131.632" y="80.006" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R92" circuitNumber="3" x="-267.772" y="184.806" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R92" circuitNumber="4" x="-131.632" y="184.806" z="0.000" rz="90.00"/>
		<Component partNumber="510-501843" refDes="R95" circuitNumber="1" x="-268.026" y="78.986" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R95" circuitNumber="2" x="-131.886" y="78.986" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R95" circuitNumber="3" x="-268.026" y="183.786" z="0.000" rz="0.00"/>
		<Component partNumber="510-501843" refDes="R95" circuitNumber="4" x="-131.886" y="183.786" z="0.000" rz="0.00"/>
		<Component partNumber="510-501867" refDes="R90" circuitNumber="1" x="-260.918" y="80.109" z="0.000" rz="0.00"/>
		<Component partNumber="510-501867" refDes="R90" circuitNumber="2" x="-124.778" y="80.109" z="0.000" rz="0.00"/>
		<Component partNumber="510-501867" refDes="R90" circuitNumber="3" x="-260.918" y="184.909" z="0.000" rz="0.00"/>
		<Component partNumber="510-501867" refDes="R90" circuitNumber="4" x="-124.778" y="184.909" z="0.000" rz="0.00"/>
		<Component partNumber="510-502050" refDes="R80" circuitNumber="1" x="-222.562" y="81.323" z="0.000" rz="0.00"/>
		<Component partNumber="510-502050" refDes="R80" circuitNumber="2" x="-86.422" y="81.323" z="0.000" rz="0.00"/>
		<Component partNumber="510-502050" refDes="R80" circuitNumber="3" x="-222.562" y="186.123" z="0.000" rz="0.00"/>
		<Component partNumber="510-502050" refDes="R80" circuitNumber="4" x="-86.422" y="186.123" z="0.000" rz="0.00"/>
		<Component partNumber="510-502089" refDes="R83" circuitNumber="1" x="-268.026" y="80.899" z="0.000" rz="0.00"/>
		<Component partNumber="510-502089" refDes="R83" circuitNumber="2" x="-131.886" y="80.899" z="0.000" rz="0.00"/>
		<Component partNumber="510-502089" refDes="R83" circuitNumber="3" x="-268.026" y="185.699" z="0.000" rz="0.00"/>
		<Component partNumber="510-502089" refDes="R83" circuitNumber="4" x="-131.886" y="185.699" z="0.000" rz="0.00"/>
		<Component partNumber="510-502141" refDes="R104" circuitNumber="1" x="-264.316" y="76.200" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R104" circuitNumber="2" x="-128.176" y="76.200" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R104" circuitNumber="3" x="-264.316" y="181.000" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R104" circuitNumber="4" x="-128.176" y="181.000" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R106" circuitNumber="1" x="-261.522" y="76.146" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R106" circuitNumber="2" x="-125.382" y="76.146" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R106" circuitNumber="3" x="-261.522" y="180.946" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R106" circuitNumber="4" x="-125.382" y="180.946" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R141" circuitNumber="1" x="-260.171" y="58.086" z="0.000" rz="90.00"/>
		<Component partNumber="510-502141" refDes="R141" circuitNumber="2" x="-124.031" y="58.086" z="0.000" rz="90.00"/>
		<Component partNumber="510-502141" refDes="R141" circuitNumber="3" x="-260.171" y="162.886" z="0.000" rz="90.00"/>
		<Component partNumber="510-502141" refDes="R141" circuitNumber="4" x="-124.031" y="162.886" z="0.000" rz="90.00"/>
		<Component partNumber="510-502141" refDes="R181" circuitNumber="1" x="-241.837" y="32.405" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R181" circuitNumber="2" x="-105.697" y="32.405" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R181" circuitNumber="3" x="-241.837" y="137.205" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R181" circuitNumber="4" x="-105.697" y="137.205" z="0.000" rz="180.00"/>
		<Component partNumber="510-502141" refDes="R182" circuitNumber="1" x="-240.567" y="32.405" z="0.000" rz="0.00"/>
		<Component partNumber="510-502141" refDes="R182" circuitNumber="2" x="-104.427" y="32.405" z="0.000" rz="0.00"/>
		<Component partNumber="510-502141" refDes="R182" circuitNumber="3" x="-240.567" y="137.205" z="0.000" rz="0.00"/>
		<Component partNumber="510-502141" refDes="R182" circuitNumber="4" x="-104.427" y="137.205" z="0.000" rz="0.00"/>
		<Component partNumber="510-502160" refDes="R85" circuitNumber="1" x="-268.026" y="81.661" z="0.000" rz="0.00"/>
		<Component partNumber="510-502160" refDes="R85" circuitNumber="2" x="-131.886" y="81.661" z="0.000" rz="0.00"/>
		<Component partNumber="510-502160" refDes="R85" circuitNumber="3" x="-268.026" y="186.461" z="0.000" rz="0.00"/>
		<Component partNumber="510-502160" refDes="R85" circuitNumber="4" x="-131.886" y="186.461" z="0.000" rz="0.00"/>
		<Component partNumber="510-502230" refDes="R124" circuitNumber="1" x="-239.538" y="55.059" z="0.000" rz="0.00"/>
		<Component partNumber="510-502230" refDes="R124" circuitNumber="2" x="-103.398" y="55.059" z="0.000" rz="0.00"/>
		<Component partNumber="510-502230" refDes="R124" circuitNumber="3" x="-239.538" y="159.859" z="0.000" rz="0.00"/>
		<Component partNumber="510-502230" refDes="R124" circuitNumber="4" x="-103.398" y="159.859" z="0.000" rz="0.00"/>
		<Component partNumber="510-502297" refDes="R51" circuitNumber="1" x="-218.342" y="92.908" z="0.000" rz="270.00"/>
		<Component partNumber="510-502297" refDes="R51" circuitNumber="2" x="-82.202" y="92.908" z="0.000" rz="270.00"/>
		<Component partNumber="510-502297" refDes="R51" circuitNumber="3" x="-218.342" y="197.708" z="0.000" rz="270.00"/>
		<Component partNumber="510-502297" refDes="R51" circuitNumber="4" x="-82.202" y="197.708" z="0.000" rz="270.00"/>
		<Component partNumber="510-502297" refDes="R68" circuitNumber="1" x="-234.296" y="85.878" z="0.000" rz="0.00"/>
		<Component partNumber="510-502297" refDes="R68" circuitNumber="2" x="-98.156" y="85.878" z="0.000" rz="0.00"/>
		<Component partNumber="510-502297" refDes="R68" circuitNumber="3" x="-234.296" y="190.678" z="0.000" rz="0.00"/>
		<Component partNumber="510-502297" refDes="R68" circuitNumber="4" x="-98.156" y="190.678" z="0.000" rz="0.00"/>
		<Component partNumber="510-502300" refDes="R200" circuitNumber="1" x="-244.999" y="17.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-502300" refDes="R200" circuitNumber="2" x="-108.859" y="17.159" z="0.000" rz="180.00"/>
		<Component partNumber="510-502300" refDes="R200" circuitNumber="3" x="-244.999" y="121.959" z="0.000" rz="180.00"/>
		<Component partNumber="510-502300" refDes="R200" circuitNumber="4" x="-108.859" y="121.959" z="0.000" rz="180.00"/>
		<Component partNumber="510-502303" refDes="R50" circuitNumber="1" x="-214.024" y="92.907" z="0.000" rz="0.00"/>
		<Component partNumber="510-502303" refDes="R50" circuitNumber="2" x="-77.884" y="92.907" z="0.000" rz="0.00"/>
		<Component partNumber="510-502303" refDes="R50" circuitNumber="3" x="-214.024" y="197.707" z="0.000" rz="0.00"/>
		<Component partNumber="510-502303" refDes="R50" circuitNumber="4" x="-77.884" y="197.707" z="0.000" rz="0.00"/>
		<Component partNumber="510-502310" refDes="R47" circuitNumber="1" x="-217.453" y="98.241" z="0.000" rz="270.00"/>
		<Component partNumber="510-502310" refDes="R47" circuitNumber="2" x="-81.313" y="98.241" z="0.000" rz="270.00"/>
		<Component partNumber="510-502310" refDes="R47" circuitNumber="3" x="-217.453" y="203.041" z="0.000" rz="270.00"/>
		<Component partNumber="510-502310" refDes="R47" circuitNumber="4" x="-81.313" y="203.041" z="0.000" rz="270.00"/>
		<Component partNumber="510-502311" refDes="R52" circuitNumber="1" x="-219.367" y="90.367" z="0.000" rz="270.00"/>
		<Component partNumber="510-502311" refDes="R52" circuitNumber="2" x="-83.227" y="90.367" z="0.000" rz="270.00"/>
		<Component partNumber="510-502311" refDes="R52" circuitNumber="3" x="-219.367" y="195.167" z="0.000" rz="270.00"/>
		<Component partNumber="510-502311" refDes="R52" circuitNumber="4" x="-83.227" y="195.167" z="0.000" rz="270.00"/>
		<Component partNumber="510-502321" refDes="R176" circuitNumber="1" x="-232.362" y="33.471" z="0.000" rz="270.00"/>
		<Component partNumber="510-502321" refDes="R176" circuitNumber="2" x="-96.222" y="33.471" z="0.000" rz="270.00"/>
		<Component partNumber="510-502321" refDes="R176" circuitNumber="3" x="-232.362" y="138.271" z="0.000" rz="270.00"/>
		<Component partNumber="510-502321" refDes="R176" circuitNumber="4" x="-96.222" y="138.271" z="0.000" rz="270.00"/>
		<Component partNumber="510-502321" refDes="R209" circuitNumber="1" x="-222.776" y="12.319" z="0.000" rz="0.00"/>
		<Component partNumber="510-502321" refDes="R209" circuitNumber="2" x="-86.636" y="12.319" z="0.000" rz="0.00"/>
		<Component partNumber="510-502321" refDes="R209" circuitNumber="3" x="-222.776" y="117.119" z="0.000" rz="0.00"/>
		<Component partNumber="510-502321" refDes="R209" circuitNumber="4" x="-86.636" y="117.119" z="0.000" rz="0.00"/>
		<Component partNumber="510-502324" refDes="R318" circuitNumber="1" x="-252.485" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R318" circuitNumber="2" x="-116.345" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R318" circuitNumber="3" x="-252.485" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R318" circuitNumber="4" x="-116.345" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R319" circuitNumber="1" x="-250.854" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R319" circuitNumber="2" x="-114.714" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R319" circuitNumber="3" x="-250.854" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R319" circuitNumber="4" x="-114.714" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R320" circuitNumber="1" x="-250.219" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R320" circuitNumber="2" x="-114.079" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R320" circuitNumber="3" x="-250.219" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R320" circuitNumber="4" x="-114.079" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R321" circuitNumber="1" x="-249.584" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R321" circuitNumber="2" x="-113.444" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R321" circuitNumber="3" x="-249.584" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R321" circuitNumber="4" x="-113.444" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R322" circuitNumber="1" x="-251.489" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R322" circuitNumber="2" x="-115.349" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R322" circuitNumber="3" x="-251.489" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R322" circuitNumber="4" x="-115.349" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R323" circuitNumber="1" x="-254.330" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R323" circuitNumber="2" x="-118.190" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R323" circuitNumber="3" x="-254.330" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R323" circuitNumber="4" x="-118.190" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R324" circuitNumber="1" x="-253.100" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R324" circuitNumber="2" x="-116.960" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R324" circuitNumber="3" x="-253.100" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R324" circuitNumber="4" x="-116.960" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R325" circuitNumber="1" x="-253.715" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R325" circuitNumber="2" x="-117.575" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R325" circuitNumber="3" x="-253.715" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R325" circuitNumber="4" x="-117.575" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R326" circuitNumber="1" x="-253.715" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R326" circuitNumber="2" x="-117.575" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R326" circuitNumber="3" x="-253.715" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R326" circuitNumber="4" x="-117.575" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R327" circuitNumber="1" x="-254.330" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R327" circuitNumber="2" x="-118.190" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R327" circuitNumber="3" x="-254.330" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R327" circuitNumber="4" x="-118.190" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R328" circuitNumber="1" x="-252.485" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R328" circuitNumber="2" x="-116.345" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R328" circuitNumber="3" x="-252.485" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R328" circuitNumber="4" x="-116.345" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R329" circuitNumber="1" x="-253.100" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R329" circuitNumber="2" x="-116.960" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R329" circuitNumber="3" x="-253.100" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R329" circuitNumber="4" x="-116.960" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R330" circuitNumber="1" x="-251.870" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R330" circuitNumber="2" x="-115.730" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R330" circuitNumber="3" x="-251.870" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R330" circuitNumber="4" x="-115.730" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R331" circuitNumber="1" x="-251.235" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R331" circuitNumber="2" x="-115.095" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R331" circuitNumber="3" x="-251.235" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R331" circuitNumber="4" x="-115.095" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R332" circuitNumber="1" x="-250.600" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R332" circuitNumber="2" x="-114.460" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R332" circuitNumber="3" x="-250.600" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R332" circuitNumber="4" x="-114.460" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R333" circuitNumber="1" x="-249.965" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R333" circuitNumber="2" x="-113.825" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R333" circuitNumber="3" x="-249.965" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R333" circuitNumber="4" x="-113.825" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R334" circuitNumber="1" x="-248.949" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R334" circuitNumber="2" x="-112.809" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R334" circuitNumber="3" x="-248.949" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R334" circuitNumber="4" x="-112.809" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R335" circuitNumber="1" x="-248.314" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R335" circuitNumber="2" x="-112.174" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R335" circuitNumber="3" x="-248.314" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R335" circuitNumber="4" x="-112.174" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R336" circuitNumber="1" x="-247.679" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R336" circuitNumber="2" x="-111.539" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R336" circuitNumber="3" x="-247.679" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R336" circuitNumber="4" x="-111.539" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R337" circuitNumber="1" x="-247.044" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R337" circuitNumber="2" x="-110.904" y="38.043" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R337" circuitNumber="3" x="-247.044" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R337" circuitNumber="4" x="-110.904" y="142.843" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R342" circuitNumber="1" x="-249.330" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R342" circuitNumber="2" x="-113.190" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R342" circuitNumber="3" x="-249.330" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R342" circuitNumber="4" x="-113.190" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R343" circuitNumber="1" x="-248.695" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R343" circuitNumber="2" x="-112.555" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R343" circuitNumber="3" x="-248.695" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R343" circuitNumber="4" x="-112.555" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R344" circuitNumber="1" x="-248.060" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R344" circuitNumber="2" x="-111.920" y="43.631" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R344" circuitNumber="3" x="-248.060" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="510-502324" refDes="R344" circuitNumber="4" x="-111.920" y="148.431" z="0.000" rz="270.00"/>
		<Component partNumber="511-500221" refDes="C18" circuitNumber="1" x="-215.728" y="99.511" z="0.000" rz="0.00"/>
		<Component partNumber="511-500221" refDes="C18" circuitNumber="2" x="-79.588" y="99.511" z="0.000" rz="0.00"/>
		<Component partNumber="511-500221" refDes="C18" circuitNumber="3" x="-215.728" y="204.311" z="0.000" rz="0.00"/>
		<Component partNumber="511-500221" refDes="C18" circuitNumber="4" x="-79.588" y="204.311" z="0.000" rz="0.00"/>
		<Component partNumber="511-500221" refDes="C370" circuitNumber="1" x="-261.388" y="58.459" z="0.000" rz="270.00"/>
		<Component partNumber="511-500221" refDes="C370" circuitNumber="2" x="-125.248" y="58.459" z="0.000" rz="270.00"/>
		<Component partNumber="511-500221" refDes="C370" circuitNumber="3" x="-261.388" y="163.259" z="0.000" rz="270.00"/>
		<Component partNumber="511-500221" refDes="C370" circuitNumber="4" x="-125.248" y="163.259" z="0.000" rz="270.00"/>
		<Component partNumber="511-500341" refDes="C242" circuitNumber="1" x="-138.171" y="75.488" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C242" circuitNumber="2" x="-2.031" y="75.488" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C242" circuitNumber="3" x="-138.171" y="180.288" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C242" circuitNumber="4" x="-2.031" y="180.288" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C318" circuitNumber="1" x="-138.171" y="67.253" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C318" circuitNumber="2" x="-2.031" y="67.253" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C318" circuitNumber="3" x="-138.171" y="172.053" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C318" circuitNumber="4" x="-2.031" y="172.053" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C350" circuitNumber="1" x="-189.616" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C350" circuitNumber="2" x="-53.476" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C350" circuitNumber="3" x="-189.616" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C350" circuitNumber="4" x="-53.476" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C356" circuitNumber="1" x="-174.122" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C356" circuitNumber="2" x="-37.982" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C356" circuitNumber="3" x="-174.122" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C356" circuitNumber="4" x="-37.982" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C357" circuitNumber="1" x="-170.566" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C357" circuitNumber="2" x="-34.426" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C357" circuitNumber="3" x="-170.566" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C357" circuitNumber="4" x="-34.426" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C363" circuitNumber="1" x="-155.199" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C363" circuitNumber="2" x="-19.059" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C363" circuitNumber="3" x="-155.199" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C363" circuitNumber="4" x="-19.059" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C371" circuitNumber="1" x="-226.089" y="53.410" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C371" circuitNumber="2" x="-89.949" y="53.410" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C371" circuitNumber="3" x="-226.089" y="158.210" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C371" circuitNumber="4" x="-89.949" y="158.210" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C410" circuitNumber="1" x="-226.089" y="36.900" z="0.000" rz="270.00"/>
		<Component partNumber="511-500341" refDes="C410" circuitNumber="2" x="-89.949" y="36.900" z="0.000" rz="270.00"/>
		<Component partNumber="511-500341" refDes="C410" circuitNumber="3" x="-226.089" y="141.700" z="0.000" rz="270.00"/>
		<Component partNumber="511-500341" refDes="C410" circuitNumber="4" x="-89.949" y="141.700" z="0.000" rz="270.00"/>
		<Component partNumber="511-500341" refDes="C437" circuitNumber="1" x="-189.616" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C437" circuitNumber="2" x="-53.476" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C437" circuitNumber="3" x="-189.616" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C437" circuitNumber="4" x="-53.476" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C443" circuitNumber="1" x="-174.122" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C443" circuitNumber="2" x="-37.982" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C443" circuitNumber="3" x="-174.122" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C443" circuitNumber="4" x="-37.982" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C444" circuitNumber="1" x="-170.566" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C444" circuitNumber="2" x="-34.426" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C444" circuitNumber="3" x="-170.566" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C444" circuitNumber="4" x="-34.426" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C450" circuitNumber="1" x="-155.199" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C450" circuitNumber="2" x="-19.059" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C450" circuitNumber="3" x="-155.199" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C450" circuitNumber="4" x="-19.059" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C46" circuitNumber="1" x="-225.038" y="90.409" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C46" circuitNumber="2" x="-88.898" y="90.409" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C46" circuitNumber="3" x="-225.038" y="195.209" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C46" circuitNumber="4" x="-88.898" y="195.209" z="0.000" rz="90.00"/>
		<Component partNumber="511-500341" refDes="C53" circuitNumber="1" x="-189.616" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C53" circuitNumber="2" x="-53.476" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C53" circuitNumber="3" x="-189.616" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C53" circuitNumber="4" x="-53.476" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C59" circuitNumber="1" x="-174.249" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C59" circuitNumber="2" x="-38.109" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C59" circuitNumber="3" x="-174.249" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C59" circuitNumber="4" x="-38.109" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C60" circuitNumber="1" x="-170.566" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C60" circuitNumber="2" x="-34.426" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C60" circuitNumber="3" x="-170.566" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C60" circuitNumber="4" x="-34.426" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500341" refDes="C66" circuitNumber="1" x="-155.209" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C66" circuitNumber="2" x="-19.069" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C66" circuitNumber="3" x="-155.209" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500341" refDes="C66" circuitNumber="4" x="-19.069" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C163" circuitNumber="1" x="-239.096" y="79.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500556" refDes="C163" circuitNumber="2" x="-102.956" y="79.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500556" refDes="C163" circuitNumber="3" x="-239.096" y="184.278" z="0.000" rz="180.00"/>
		<Component partNumber="511-500556" refDes="C163" circuitNumber="4" x="-102.956" y="184.278" z="0.000" rz="180.00"/>
		<Component partNumber="511-500556" refDes="C183" circuitNumber="1" x="-232.771" y="77.980" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C183" circuitNumber="2" x="-96.631" y="77.980" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C183" circuitNumber="3" x="-232.771" y="182.780" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C183" circuitNumber="4" x="-96.631" y="182.780" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C246" circuitNumber="1" x="-232.773" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C246" circuitNumber="2" x="-96.633" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C246" circuitNumber="3" x="-232.773" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C246" circuitNumber="4" x="-96.633" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500556" refDes="C300" circuitNumber="1" x="-237.896" y="71.078" z="0.000" rz="270.00"/>
		<Component partNumber="511-500556" refDes="C300" circuitNumber="2" x="-101.756" y="71.078" z="0.000" rz="270.00"/>
		<Component partNumber="511-500556" refDes="C300" circuitNumber="3" x="-237.896" y="175.878" z="0.000" rz="270.00"/>
		<Component partNumber="511-500556" refDes="C300" circuitNumber="4" x="-101.756" y="175.878" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C426" circuitNumber="1" x="-247.679" y="32.963" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C426" circuitNumber="2" x="-111.539" y="32.963" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C426" circuitNumber="3" x="-247.679" y="137.763" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C426" circuitNumber="4" x="-111.539" y="137.763" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C427" circuitNumber="1" x="-246.105" y="32.963" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C427" circuitNumber="2" x="-109.965" y="32.963" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C427" circuitNumber="3" x="-246.105" y="137.763" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C427" circuitNumber="4" x="-109.965" y="137.763" z="0.000" rz="90.00"/>
		<Component partNumber="511-500561" refDes="C433" circuitNumber="1" x="-247.679" y="29.915" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C433" circuitNumber="2" x="-111.539" y="29.915" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C433" circuitNumber="3" x="-247.679" y="134.715" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C433" circuitNumber="4" x="-111.539" y="134.715" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C434" circuitNumber="1" x="-246.105" y="29.915" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C434" circuitNumber="2" x="-109.965" y="29.915" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C434" circuitNumber="3" x="-246.105" y="134.715" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C434" circuitNumber="4" x="-109.965" y="134.715" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C452" circuitNumber="1" x="-224.692" y="21.787" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C452" circuitNumber="2" x="-88.552" y="21.787" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C452" circuitNumber="3" x="-224.692" y="126.587" z="0.000" rz="270.00"/>
		<Component partNumber="511-500561" refDes="C452" circuitNumber="4" x="-88.552" y="126.587" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C113" circuitNumber="1" x="-192.918" y="82.620" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C113" circuitNumber="2" x="-56.778" y="82.620" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C113" circuitNumber="3" x="-192.918" y="187.420" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C113" circuitNumber="4" x="-56.778" y="187.420" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C171" circuitNumber="1" x="-192.918" y="79.191" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C171" circuitNumber="2" x="-56.778" y="79.191" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C171" circuitNumber="3" x="-192.918" y="183.991" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C171" circuitNumber="4" x="-56.778" y="183.991" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C39" circuitNumber="1" x="-152.913" y="93.034" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C39" circuitNumber="2" x="-16.773" y="93.034" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C39" circuitNumber="3" x="-152.913" y="197.834" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C39" circuitNumber="4" x="-16.773" y="197.834" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C408" circuitNumber="1" x="-152.913" y="39.186" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C408" circuitNumber="2" x="-16.773" y="39.186" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C408" circuitNumber="3" x="-152.913" y="143.986" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C408" circuitNumber="4" x="-16.773" y="143.986" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C411" circuitNumber="1" x="-192.918" y="36.265" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C411" circuitNumber="2" x="-56.778" y="36.265" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C411" circuitNumber="3" x="-192.918" y="141.065" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C411" circuitNumber="4" x="-56.778" y="141.065" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C428" circuitNumber="1" x="-192.918" y="32.836" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C428" circuitNumber="2" x="-56.778" y="32.836" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C428" circuitNumber="3" x="-192.918" y="137.636" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C428" circuitNumber="4" x="-56.778" y="137.636" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C432" circuitNumber="1" x="-152.913" y="30.169" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C432" circuitNumber="2" x="-16.773" y="30.169" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C432" circuitNumber="3" x="-152.913" y="134.969" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C432" circuitNumber="4" x="-16.773" y="134.969" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C51" circuitNumber="1" x="-152.913" y="89.605" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C51" circuitNumber="2" x="-16.773" y="89.605" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C51" circuitNumber="3" x="-152.913" y="194.405" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C51" circuitNumber="4" x="-16.773" y="194.405" z="0.000" rz="270.00"/>
		<Component partNumber="511-500568" refDes="C82" circuitNumber="1" x="-259.104" y="85.576" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C82" circuitNumber="2" x="-122.964" y="85.576" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C82" circuitNumber="3" x="-259.104" y="190.376" z="0.000" rz="90.00"/>
		<Component partNumber="511-500568" refDes="C82" circuitNumber="4" x="-122.964" y="190.376" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C106" circuitNumber="1" x="-251.915" y="83.327" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C106" circuitNumber="2" x="-115.775" y="83.327" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C106" circuitNumber="3" x="-251.915" y="188.127" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C106" circuitNumber="4" x="-115.775" y="188.127" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C141" circuitNumber="1" x="-243.708" y="79.059" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C141" circuitNumber="2" x="-107.568" y="79.059" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C141" circuitNumber="3" x="-243.708" y="183.859" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C141" circuitNumber="4" x="-107.568" y="183.859" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C192" circuitNumber="1" x="-259.988" y="78.589" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C192" circuitNumber="2" x="-123.848" y="78.589" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C192" circuitNumber="3" x="-259.988" y="183.389" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C192" circuitNumber="4" x="-123.848" y="183.389" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C225" circuitNumber="1" x="-240.669" y="75.209" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C225" circuitNumber="2" x="-104.529" y="75.209" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C225" circuitNumber="3" x="-240.669" y="180.009" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C225" circuitNumber="4" x="-104.529" y="180.009" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C254" circuitNumber="1" x="-187.457" y="73.476" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C254" circuitNumber="2" x="-51.317" y="73.476" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C254" circuitNumber="3" x="-187.457" y="178.276" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C254" circuitNumber="4" x="-51.317" y="178.276" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C259" circuitNumber="1" x="-168.534" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C259" circuitNumber="2" x="-32.394" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C259" circuitNumber="3" x="-168.534" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C259" circuitNumber="4" x="-32.394" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C263" circuitNumber="1" x="-156.850" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C263" circuitNumber="2" x="-20.710" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C263" circuitNumber="3" x="-156.850" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C263" circuitNumber="4" x="-20.710" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C342" circuitNumber="1" x="-254.388" y="58.239" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C342" circuitNumber="2" x="-118.248" y="58.239" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C342" circuitNumber="3" x="-254.388" y="163.039" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C342" circuitNumber="4" x="-118.248" y="163.039" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C369" circuitNumber="1" x="-257.504" y="56.561" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C369" circuitNumber="2" x="-121.364" y="56.561" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C369" circuitNumber="3" x="-257.504" y="161.361" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C369" circuitNumber="4" x="-121.364" y="161.361" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C372" circuitNumber="1" x="-153.040" y="52.902" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C372" circuitNumber="2" x="-16.900" y="52.902" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C372" circuitNumber="3" x="-153.040" y="157.702" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C372" circuitNumber="4" x="-16.900" y="157.702" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C385" circuitNumber="1" x="-187.457" y="42.488" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C385" circuitNumber="2" x="-51.317" y="42.488" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C385" circuitNumber="3" x="-187.457" y="147.288" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C385" circuitNumber="4" x="-51.317" y="147.288" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C395" circuitNumber="1" x="-156.596" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C395" circuitNumber="2" x="-20.456" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C395" circuitNumber="3" x="-156.596" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C395" circuitNumber="4" x="-20.456" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C396" circuitNumber="1" x="-154.310" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C396" circuitNumber="2" x="-18.170" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C396" circuitNumber="3" x="-154.310" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C396" circuitNumber="4" x="-18.170" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C40" circuitNumber="1" x="-219.538" y="93.028" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C40" circuitNumber="2" x="-83.398" y="93.028" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C40" circuitNumber="3" x="-219.538" y="197.828" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C40" circuitNumber="4" x="-83.398" y="197.828" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C429" circuitNumber="1" x="-253.902" y="31.312" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C429" circuitNumber="2" x="-117.762" y="31.312" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C429" circuitNumber="3" x="-253.902" y="136.112" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C429" circuitNumber="4" x="-117.762" y="136.112" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C43" circuitNumber="1" x="-245.038" y="76.216" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C43" circuitNumber="2" x="-108.898" y="76.216" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C43" circuitNumber="3" x="-245.038" y="181.016" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C43" circuitNumber="4" x="-108.898" y="181.016" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C459" circuitNumber="1" x="-189.997" y="12.034" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C459" circuitNumber="2" x="-53.857" y="12.034" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C459" circuitNumber="3" x="-189.997" y="116.834" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C459" circuitNumber="4" x="-53.857" y="116.834" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C464" circuitNumber="1" x="-174.630" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C464" circuitNumber="2" x="-38.490" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C464" circuitNumber="3" x="-174.630" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C464" circuitNumber="4" x="-38.490" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C470" circuitNumber="1" x="-154.564" y="12.034" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C470" circuitNumber="2" x="-18.424" y="12.034" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C470" circuitNumber="3" x="-154.564" y="116.834" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C470" circuitNumber="4" x="-18.424" y="116.834" z="0.000" rz="180.00"/>
		<Component partNumber="511-500572" refDes="C49" circuitNumber="1" x="-245.638" y="80.978" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C49" circuitNumber="2" x="-109.498" y="80.978" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C49" circuitNumber="3" x="-245.638" y="185.778" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C49" circuitNumber="4" x="-109.498" y="185.778" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C5" circuitNumber="1" x="-175.392" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C5" circuitNumber="2" x="-39.252" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C5" circuitNumber="3" x="-175.392" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C5" circuitNumber="4" x="-39.252" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C50" circuitNumber="1" x="-240.688" y="81.827" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C50" circuitNumber="2" x="-104.548" y="81.827" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C50" circuitNumber="3" x="-240.688" y="186.627" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C50" circuitNumber="4" x="-104.548" y="186.627" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C6" circuitNumber="1" x="-168.026" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C6" circuitNumber="2" x="-31.886" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C6" circuitNumber="3" x="-168.026" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C6" circuitNumber="4" x="-31.886" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500572" refDes="C67" circuitNumber="1" x="-220.288" y="87.939" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C67" circuitNumber="2" x="-84.148" y="87.939" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C67" circuitNumber="3" x="-220.288" y="192.739" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C67" circuitNumber="4" x="-84.148" y="192.739" z="0.000" rz="270.00"/>
		<Component partNumber="511-500572" refDes="C70" circuitNumber="1" x="-153.040" y="87.184" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C70" circuitNumber="2" x="-16.900" y="87.184" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C70" circuitNumber="3" x="-153.040" y="191.984" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C70" circuitNumber="4" x="-16.900" y="191.984" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C84" circuitNumber="1" x="-138.054" y="86.029" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C84" circuitNumber="2" x="-1.914" y="86.029" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C84" circuitNumber="3" x="-138.054" y="190.829" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C84" circuitNumber="4" x="-1.914" y="190.829" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C89" circuitNumber="1" x="-228.338" y="83.738" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C89" circuitNumber="2" x="-92.198" y="83.738" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C89" circuitNumber="3" x="-228.338" y="188.538" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C89" circuitNumber="4" x="-92.198" y="188.538" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C90" circuitNumber="1" x="-223.549" y="83.890" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C90" circuitNumber="2" x="-87.409" y="83.890" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C90" circuitNumber="3" x="-223.549" y="188.690" z="0.000" rz="90.00"/>
		<Component partNumber="511-500572" refDes="C90" circuitNumber="4" x="-87.409" y="188.690" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C111" circuitNumber="1" x="-244.138" y="87.628" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C111" circuitNumber="2" x="-107.998" y="87.628" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C111" circuitNumber="3" x="-244.138" y="192.428" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C111" circuitNumber="4" x="-107.998" y="192.428" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C152" circuitNumber="1" x="-221.094" y="80.278" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C152" circuitNumber="2" x="-84.954" y="80.278" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C152" circuitNumber="3" x="-221.094" y="185.078" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C152" circuitNumber="4" x="-84.954" y="185.078" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C191" circuitNumber="1" x="-219.688" y="77.489" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C191" circuitNumber="2" x="-83.548" y="77.489" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C191" circuitNumber="3" x="-219.688" y="182.289" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C191" circuitNumber="4" x="-83.548" y="182.289" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C244" circuitNumber="1" x="-235.096" y="73.877" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C244" circuitNumber="2" x="-98.956" y="73.877" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C244" circuitNumber="3" x="-235.096" y="178.677" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C244" circuitNumber="4" x="-98.956" y="178.677" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C253" circuitNumber="1" x="-189.743" y="73.476" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C253" circuitNumber="2" x="-53.603" y="73.476" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C253" circuitNumber="3" x="-189.743" y="178.276" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C253" circuitNumber="4" x="-53.603" y="178.276" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C258" circuitNumber="1" x="-175.773" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C258" circuitNumber="2" x="-39.633" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C258" circuitNumber="3" x="-175.773" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C258" circuitNumber="4" x="-39.633" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C264" circuitNumber="1" x="-154.691" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C264" circuitNumber="2" x="-18.551" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C264" circuitNumber="3" x="-154.691" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C264" circuitNumber="4" x="-18.551" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C265" circuitNumber="1" x="-240.821" y="72.549" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C265" circuitNumber="2" x="-104.681" y="72.549" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C265" circuitNumber="3" x="-240.821" y="177.349" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C265" circuitNumber="4" x="-104.681" y="177.349" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C284" circuitNumber="1" x="-234.818" y="71.889" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C284" circuitNumber="2" x="-98.678" y="71.889" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C284" circuitNumber="3" x="-234.818" y="176.689" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C284" circuitNumber="4" x="-98.678" y="176.689" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C286" circuitNumber="1" x="-251.915" y="71.830" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C286" circuitNumber="2" x="-115.775" y="71.830" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C286" circuitNumber="3" x="-251.915" y="176.630" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C286" circuitNumber="4" x="-115.775" y="176.630" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C293" circuitNumber="1" x="-244.123" y="71.825" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C293" circuitNumber="2" x="-107.983" y="71.825" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C293" circuitNumber="3" x="-244.123" y="176.625" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C293" circuitNumber="4" x="-107.983" y="176.625" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C3" circuitNumber="1" x="-188.219" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C3" circuitNumber="2" x="-52.079" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C3" circuitNumber="3" x="-188.219" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C3" circuitNumber="4" x="-52.079" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C301" circuitNumber="1" x="-225.494" y="70.976" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C301" circuitNumber="2" x="-89.354" y="70.976" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C301" circuitNumber="3" x="-225.494" y="175.776" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C301" circuitNumber="4" x="-89.354" y="175.776" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C302" circuitNumber="1" x="-229.495" y="70.676" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C302" circuitNumber="2" x="-93.355" y="70.676" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C302" circuitNumber="3" x="-229.495" y="175.476" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C302" circuitNumber="4" x="-93.355" y="175.476" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C303" circuitNumber="1" x="-227.588" y="70.289" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C303" circuitNumber="2" x="-91.448" y="70.289" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C303" circuitNumber="3" x="-227.588" y="175.089" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C303" circuitNumber="4" x="-91.448" y="175.089" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C304" circuitNumber="1" x="-223.494" y="70.276" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C304" circuitNumber="2" x="-87.354" y="70.276" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C304" circuitNumber="3" x="-223.494" y="175.076" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C304" circuitNumber="4" x="-87.354" y="175.076" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C305" circuitNumber="1" x="-225.494" y="69.776" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C305" circuitNumber="2" x="-89.354" y="69.776" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C305" circuitNumber="3" x="-225.494" y="174.576" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C305" circuitNumber="4" x="-89.354" y="174.576" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C307" circuitNumber="1" x="-232.188" y="70.189" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C307" circuitNumber="2" x="-96.048" y="70.189" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C307" circuitNumber="3" x="-232.188" y="174.989" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C307" circuitNumber="4" x="-96.048" y="174.989" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C308" circuitNumber="1" x="-220.738" y="69.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C308" circuitNumber="2" x="-84.598" y="69.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C308" circuitNumber="3" x="-220.738" y="174.659" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C308" circuitNumber="4" x="-84.598" y="174.659" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C309" circuitNumber="1" x="-218.694" y="69.476" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C309" circuitNumber="2" x="-82.554" y="69.476" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C309" circuitNumber="3" x="-218.694" y="174.276" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C309" circuitNumber="4" x="-82.554" y="174.276" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C312" circuitNumber="1" x="-229.095" y="68.676" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C312" circuitNumber="2" x="-92.955" y="68.676" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C312" circuitNumber="3" x="-229.095" y="173.476" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C312" circuitNumber="4" x="-92.955" y="173.476" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C316" circuitNumber="1" x="-222.914" y="67.507" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C316" circuitNumber="2" x="-86.774" y="67.507" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C316" circuitNumber="3" x="-222.914" y="172.307" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C316" circuitNumber="4" x="-86.774" y="172.307" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C328" circuitNumber="1" x="-235.488" y="65.478" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C328" circuitNumber="2" x="-99.348" y="65.478" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C328" circuitNumber="3" x="-235.488" y="170.278" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C328" circuitNumber="4" x="-99.348" y="170.278" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C33" circuitNumber="1" x="-257.358" y="95.159" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C33" circuitNumber="2" x="-121.218" y="95.159" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C33" circuitNumber="3" x="-257.358" y="199.959" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C33" circuitNumber="4" x="-121.218" y="199.959" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C331" circuitNumber="1" x="-238.042" y="64.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C331" circuitNumber="2" x="-101.902" y="64.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C331" circuitNumber="3" x="-238.042" y="169.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C331" circuitNumber="4" x="-101.902" y="169.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C343" circuitNumber="1" x="-235.360" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C343" circuitNumber="2" x="-99.220" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C343" circuitNumber="3" x="-235.360" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C343" circuitNumber="4" x="-99.220" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C366" circuitNumber="1" x="-138.054" y="55.315" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C366" circuitNumber="2" x="-1.914" y="55.315" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C366" circuitNumber="3" x="-138.054" y="160.115" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C366" circuitNumber="4" x="-1.914" y="160.115" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C367" circuitNumber="1" x="-153.040" y="55.188" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C367" circuitNumber="2" x="-16.900" y="55.188" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C367" circuitNumber="3" x="-153.040" y="159.988" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C367" circuitNumber="4" x="-16.900" y="159.988" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C375" circuitNumber="1" x="-247.298" y="48.838" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C375" circuitNumber="2" x="-111.158" y="48.838" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C375" circuitNumber="3" x="-247.298" y="153.638" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C375" circuitNumber="4" x="-111.158" y="153.638" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C377" circuitNumber="1" x="-210.722" y="48.711" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C377" circuitNumber="2" x="-74.582" y="48.711" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C377" circuitNumber="3" x="-210.722" y="153.511" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C377" circuitNumber="4" x="-74.582" y="153.511" z="0.000" rz="270.00"/>
		<Component partNumber="511-500589" refDes="C384" circuitNumber="1" x="-242.538" y="43.278" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C384" circuitNumber="2" x="-106.398" y="43.278" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C384" circuitNumber="3" x="-242.538" y="148.078" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C384" circuitNumber="4" x="-106.398" y="148.078" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C390" circuitNumber="1" x="-175.646" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C390" circuitNumber="2" x="-39.506" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C390" circuitNumber="3" x="-175.646" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C390" circuitNumber="4" x="-39.506" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C391" circuitNumber="1" x="-168.534" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C391" circuitNumber="2" x="-32.394" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C391" circuitNumber="3" x="-168.534" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C391" circuitNumber="4" x="-32.394" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C4" circuitNumber="1" x="-184.917" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C4" circuitNumber="2" x="-48.777" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C4" circuitNumber="3" x="-184.917" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C4" circuitNumber="4" x="-48.777" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C406" circuitNumber="1" x="-210.595" y="40.075" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C406" circuitNumber="2" x="-74.455" y="40.075" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C406" circuitNumber="3" x="-210.595" y="144.875" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C406" circuitNumber="4" x="-74.455" y="144.875" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C460" circuitNumber="1" x="-187.711" y="12.034" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C460" circuitNumber="2" x="-51.571" y="12.034" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C460" circuitNumber="3" x="-187.711" y="116.834" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C460" circuitNumber="4" x="-51.571" y="116.834" z="0.000" rz="180.00"/>
		<Component partNumber="511-500589" refDes="C465" circuitNumber="1" x="-168.534" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C465" circuitNumber="2" x="-32.394" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C465" circuitNumber="3" x="-168.534" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C465" circuitNumber="4" x="-32.394" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C469" circuitNumber="1" x="-156.850" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C469" circuitNumber="2" x="-20.710" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C469" circuitNumber="3" x="-156.850" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C469" circuitNumber="4" x="-20.710" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C52" circuitNumber="1" x="-216.669" y="89.059" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C52" circuitNumber="2" x="-80.529" y="89.059" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C52" circuitNumber="3" x="-216.669" y="193.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C52" circuitNumber="4" x="-80.529" y="193.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C7" circuitNumber="1" x="-160.660" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C7" circuitNumber="2" x="-24.520" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C7" circuitNumber="3" x="-160.660" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C7" circuitNumber="4" x="-24.520" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C8" circuitNumber="1" x="-156.596" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C8" circuitNumber="2" x="-20.456" y="103.829" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C8" circuitNumber="3" x="-156.596" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C8" circuitNumber="4" x="-20.456" y="208.629" z="0.000" rz="0.00"/>
		<Component partNumber="511-500589" refDes="C88" circuitNumber="1" x="-228.338" y="85.575" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C88" circuitNumber="2" x="-92.198" y="85.575" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C88" circuitNumber="3" x="-228.338" y="190.375" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C88" circuitNumber="4" x="-92.198" y="190.375" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C99" circuitNumber="1" x="-244.123" y="83.509" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C99" circuitNumber="2" x="-107.983" y="83.509" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C99" circuitNumber="3" x="-244.123" y="188.309" z="0.000" rz="90.00"/>
		<Component partNumber="511-500589" refDes="C99" circuitNumber="4" x="-107.983" y="188.309" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C100" circuitNumber="1" x="-257.966" y="83.382" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C100" circuitNumber="2" x="-121.826" y="83.382" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C100" circuitNumber="3" x="-257.966" y="188.182" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C100" circuitNumber="4" x="-121.826" y="188.182" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C101" circuitNumber="1" x="-255.934" y="83.382" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C101" circuitNumber="2" x="-119.794" y="83.382" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C101" circuitNumber="3" x="-255.934" y="188.182" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C101" circuitNumber="4" x="-119.794" y="188.182" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C109" circuitNumber="1" x="-138.054" y="84.125" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C109" circuitNumber="2" x="-1.914" y="84.125" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C109" circuitNumber="3" x="-138.054" y="188.925" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C109" circuitNumber="4" x="-1.914" y="188.925" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C112" circuitNumber="1" x="-217.496" y="82.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C112" circuitNumber="2" x="-81.356" y="82.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C112" circuitNumber="3" x="-217.496" y="187.478" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C112" circuitNumber="4" x="-81.356" y="187.478" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C123" circuitNumber="1" x="-239.096" y="81.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C123" circuitNumber="2" x="-102.956" y="81.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C123" circuitNumber="3" x="-239.096" y="186.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C123" circuitNumber="4" x="-102.956" y="186.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C137" circuitNumber="1" x="-224.469" y="81.230" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C137" circuitNumber="2" x="-88.329" y="81.230" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C137" circuitNumber="3" x="-224.469" y="186.030" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C137" circuitNumber="4" x="-88.329" y="186.030" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C145" circuitNumber="1" x="-225.769" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C145" circuitNumber="2" x="-89.629" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C145" circuitNumber="3" x="-225.769" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C145" circuitNumber="4" x="-89.629" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C146" circuitNumber="1" x="-224.469" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C146" circuitNumber="2" x="-88.329" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C146" circuitNumber="3" x="-224.469" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C146" circuitNumber="4" x="-88.329" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C148" circuitNumber="1" x="-232.891" y="80.580" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C148" circuitNumber="2" x="-96.751" y="80.580" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C148" circuitNumber="3" x="-232.891" y="185.380" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C148" circuitNumber="4" x="-96.751" y="185.380" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C149" circuitNumber="1" x="-231.471" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C149" circuitNumber="2" x="-95.331" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C149" circuitNumber="3" x="-231.471" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C149" circuitNumber="4" x="-95.331" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C151" circuitNumber="1" x="-228.871" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C151" circuitNumber="2" x="-92.731" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C151" circuitNumber="3" x="-228.871" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C151" circuitNumber="4" x="-92.731" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C154" circuitNumber="1" x="-227.070" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C154" circuitNumber="2" x="-90.930" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C154" circuitNumber="3" x="-227.070" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C154" circuitNumber="4" x="-90.930" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C156" circuitNumber="1" x="-224.470" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C156" circuitNumber="2" x="-88.330" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C156" circuitNumber="3" x="-224.470" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C156" circuitNumber="4" x="-88.330" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C167" circuitNumber="1" x="-224.469" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C167" circuitNumber="2" x="-88.329" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C167" circuitNumber="3" x="-224.469" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C167" circuitNumber="4" x="-88.329" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C168" circuitNumber="1" x="-231.471" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C168" circuitNumber="2" x="-95.331" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C168" circuitNumber="3" x="-231.471" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C168" circuitNumber="4" x="-95.331" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C170" circuitNumber="1" x="-228.871" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C170" circuitNumber="2" x="-92.731" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C170" circuitNumber="3" x="-228.871" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C170" circuitNumber="4" x="-92.731" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C173" circuitNumber="1" x="-233.747" y="78.955" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C173" circuitNumber="2" x="-97.607" y="78.955" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C173" circuitNumber="3" x="-233.747" y="183.755" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C173" circuitNumber="4" x="-97.607" y="183.755" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C174" circuitNumber="1" x="-223.091" y="78.683" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C174" circuitNumber="2" x="-86.951" y="78.683" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C174" circuitNumber="3" x="-223.091" y="183.483" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C174" circuitNumber="4" x="-86.951" y="183.483" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C175" circuitNumber="1" x="-217.496" y="78.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C175" circuitNumber="2" x="-81.356" y="78.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C175" circuitNumber="3" x="-217.496" y="183.478" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C175" circuitNumber="4" x="-81.356" y="183.478" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C177" circuitNumber="1" x="-225.770" y="78.629" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C177" circuitNumber="2" x="-89.630" y="78.629" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C177" circuitNumber="3" x="-225.770" y="183.429" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C177" circuitNumber="4" x="-89.630" y="183.429" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C178" circuitNumber="1" x="-224.469" y="78.629" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C178" circuitNumber="2" x="-88.329" y="78.629" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C178" circuitNumber="3" x="-224.469" y="183.429" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C178" circuitNumber="4" x="-88.329" y="183.429" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C181" circuitNumber="1" x="-230.171" y="78.630" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C181" circuitNumber="2" x="-94.031" y="78.630" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C181" circuitNumber="3" x="-230.171" y="183.430" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C181" circuitNumber="4" x="-94.031" y="183.430" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C184" circuitNumber="1" x="-227.070" y="77.979" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C184" circuitNumber="2" x="-90.930" y="77.979" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C184" circuitNumber="3" x="-227.070" y="182.779" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C184" circuitNumber="4" x="-90.930" y="182.779" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C187" circuitNumber="1" x="-231.471" y="77.980" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C187" circuitNumber="2" x="-95.331" y="77.980" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C187" circuitNumber="3" x="-231.471" y="182.780" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C187" circuitNumber="4" x="-95.331" y="182.780" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C196" circuitNumber="1" x="-232.771" y="77.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C196" circuitNumber="2" x="-96.631" y="77.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C196" circuitNumber="3" x="-232.771" y="182.130" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C196" circuitNumber="4" x="-96.631" y="182.130" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C198" circuitNumber="1" x="-230.171" y="77.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C198" circuitNumber="2" x="-94.031" y="77.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C198" circuitNumber="3" x="-230.171" y="182.130" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C198" circuitNumber="4" x="-94.031" y="182.130" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C200" circuitNumber="1" x="-225.770" y="77.329" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C200" circuitNumber="2" x="-89.630" y="77.329" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C200" circuitNumber="3" x="-225.770" y="182.129" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C200" circuitNumber="4" x="-89.630" y="182.129" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C201" circuitNumber="1" x="-224.469" y="77.329" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C201" circuitNumber="2" x="-88.329" y="77.329" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C201" circuitNumber="3" x="-224.469" y="182.129" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C201" circuitNumber="4" x="-88.329" y="182.129" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C205" circuitNumber="1" x="-238.188" y="77.109" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C205" circuitNumber="2" x="-102.048" y="77.109" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C205" circuitNumber="3" x="-238.188" y="181.909" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C205" circuitNumber="4" x="-102.048" y="181.909" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C206" circuitNumber="1" x="-232.773" y="76.127" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C206" circuitNumber="2" x="-96.633" y="76.127" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C206" circuitNumber="3" x="-232.773" y="180.927" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C206" circuitNumber="4" x="-96.633" y="180.927" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C207" circuitNumber="1" x="-231.473" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C207" circuitNumber="2" x="-95.333" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C207" circuitNumber="3" x="-231.473" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C207" circuitNumber="4" x="-95.333" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C209" circuitNumber="1" x="-228.872" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C209" circuitNumber="2" x="-92.732" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C209" circuitNumber="3" x="-228.872" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C209" circuitNumber="4" x="-92.732" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C211" circuitNumber="1" x="-225.769" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C211" circuitNumber="2" x="-89.629" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C211" circuitNumber="3" x="-225.769" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C211" circuitNumber="4" x="-89.629" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C212" circuitNumber="1" x="-224.469" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C212" circuitNumber="2" x="-88.329" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C212" circuitNumber="3" x="-224.469" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C212" circuitNumber="4" x="-88.329" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C215" circuitNumber="1" x="-224.469" y="75.528" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C215" circuitNumber="2" x="-88.329" y="75.528" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C215" circuitNumber="3" x="-224.469" y="180.328" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C215" circuitNumber="4" x="-88.329" y="180.328" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C220" circuitNumber="1" x="-227.070" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C220" circuitNumber="2" x="-90.930" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C220" circuitNumber="3" x="-227.070" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C220" circuitNumber="4" x="-90.930" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C226" circuitNumber="1" x="-224.470" y="74.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C226" circuitNumber="2" x="-88.330" y="74.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C226" circuitNumber="3" x="-224.470" y="179.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C226" circuitNumber="4" x="-88.330" y="179.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C230" circuitNumber="1" x="-228.872" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C230" circuitNumber="2" x="-92.732" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C230" circuitNumber="3" x="-228.872" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C230" circuitNumber="4" x="-92.732" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C233" circuitNumber="1" x="-223.168" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C233" circuitNumber="2" x="-87.028" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C233" circuitNumber="3" x="-223.168" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C233" circuitNumber="4" x="-87.028" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C239" circuitNumber="1" x="-224.469" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C239" circuitNumber="2" x="-88.329" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C239" circuitNumber="3" x="-224.469" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C239" circuitNumber="4" x="-88.329" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C243" circuitNumber="1" x="-233.748" y="73.902" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C243" circuitNumber="2" x="-97.608" y="73.902" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C243" circuitNumber="3" x="-233.748" y="178.702" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C243" circuitNumber="4" x="-97.608" y="178.702" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C247" circuitNumber="1" x="-230.172" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C247" circuitNumber="2" x="-94.032" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C247" circuitNumber="3" x="-230.172" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C247" circuitNumber="4" x="-94.032" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C248" circuitNumber="1" x="-228.872" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C248" circuitNumber="2" x="-92.732" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C248" circuitNumber="3" x="-228.872" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C248" circuitNumber="4" x="-92.732" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C251" circuitNumber="1" x="-224.469" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C251" circuitNumber="2" x="-88.329" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C251" circuitNumber="3" x="-224.469" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C251" circuitNumber="4" x="-88.329" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C257" circuitNumber="1" x="-177.678" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C257" circuitNumber="2" x="-41.538" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C257" circuitNumber="3" x="-177.678" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C257" circuitNumber="4" x="-41.538" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C262" circuitNumber="1" x="-158.755" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C262" circuitNumber="2" x="-22.615" y="73.349" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C262" circuitNumber="3" x="-158.755" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C262" circuitNumber="4" x="-22.615" y="178.149" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C266" circuitNumber="1" x="-217.496" y="73.078" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C266" circuitNumber="2" x="-81.356" y="73.078" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C266" circuitNumber="3" x="-217.496" y="177.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C266" circuitNumber="4" x="-81.356" y="177.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C267" circuitNumber="1" x="-224.469" y="72.928" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C267" circuitNumber="2" x="-88.329" y="72.928" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C267" circuitNumber="3" x="-224.469" y="177.728" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C267" circuitNumber="4" x="-88.329" y="177.728" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C268" circuitNumber="1" x="-232.773" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C268" circuitNumber="2" x="-96.633" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C268" circuitNumber="3" x="-232.773" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C268" circuitNumber="4" x="-96.633" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C270" circuitNumber="1" x="-230.172" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C270" circuitNumber="2" x="-94.032" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C270" circuitNumber="3" x="-230.172" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C270" circuitNumber="4" x="-94.032" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C272" circuitNumber="1" x="-227.070" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C272" circuitNumber="2" x="-90.930" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C272" circuitNumber="3" x="-227.070" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C272" circuitNumber="4" x="-90.930" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C275" circuitNumber="1" x="-223.174" y="72.277" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C275" circuitNumber="2" x="-87.034" y="72.277" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C275" circuitNumber="3" x="-223.174" y="177.077" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C275" circuitNumber="4" x="-87.034" y="177.077" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C276" circuitNumber="1" x="-223.183" y="72.927" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C276" circuitNumber="2" x="-87.043" y="72.927" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C276" circuitNumber="3" x="-223.183" y="177.727" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C276" circuitNumber="4" x="-87.043" y="177.727" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C278" circuitNumber="1" x="-231.473" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C278" circuitNumber="2" x="-95.333" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C278" circuitNumber="3" x="-231.473" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C278" circuitNumber="4" x="-95.333" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C280" circuitNumber="1" x="-228.872" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C280" circuitNumber="2" x="-92.732" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C280" circuitNumber="3" x="-228.872" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C280" circuitNumber="4" x="-92.732" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C282" circuitNumber="1" x="-225.769" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C282" circuitNumber="2" x="-89.629" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C282" circuitNumber="3" x="-225.769" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C282" circuitNumber="4" x="-89.629" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C285" circuitNumber="1" x="-238.965" y="71.735" z="0.000" rz="135.00"/>
		<Component partNumber="511-500590" refDes="C285" circuitNumber="2" x="-102.825" y="71.735" z="0.000" rz="135.00"/>
		<Component partNumber="511-500590" refDes="C285" circuitNumber="3" x="-238.965" y="176.535" z="0.000" rz="135.00"/>
		<Component partNumber="511-500590" refDes="C285" circuitNumber="4" x="-102.825" y="176.535" z="0.000" rz="135.00"/>
		<Component partNumber="511-500590" refDes="C287" circuitNumber="1" x="-257.712" y="71.825" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C287" circuitNumber="2" x="-121.572" y="71.825" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C287" circuitNumber="3" x="-257.712" y="176.625" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C287" circuitNumber="4" x="-121.572" y="176.625" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C297" circuitNumber="1" x="-233.798" y="71.159" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C297" circuitNumber="2" x="-97.658" y="71.159" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C297" circuitNumber="3" x="-233.798" y="175.959" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C297" circuitNumber="4" x="-97.658" y="175.959" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C310" circuitNumber="1" x="-217.496" y="69.078" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C310" circuitNumber="2" x="-81.356" y="69.078" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C310" circuitNumber="3" x="-217.496" y="173.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C310" circuitNumber="4" x="-81.356" y="173.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C314" circuitNumber="1" x="-221.096" y="67.878" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C314" circuitNumber="2" x="-84.956" y="67.878" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C314" circuitNumber="3" x="-221.096" y="172.678" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C314" circuitNumber="4" x="-84.956" y="172.678" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C315" circuitNumber="1" x="-239.096" y="67.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C315" circuitNumber="2" x="-102.956" y="67.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C315" circuitNumber="3" x="-239.096" y="172.278" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C315" circuitNumber="4" x="-102.956" y="172.278" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C317" circuitNumber="1" x="-230.296" y="67.588" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C317" circuitNumber="2" x="-94.156" y="67.588" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C317" circuitNumber="3" x="-230.296" y="172.388" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C317" circuitNumber="4" x="-94.156" y="172.388" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C319" circuitNumber="1" x="-237.365" y="66.935" z="0.000" rz="135.00"/>
		<Component partNumber="511-500590" refDes="C319" circuitNumber="2" x="-101.225" y="66.935" z="0.000" rz="135.00"/>
		<Component partNumber="511-500590" refDes="C319" circuitNumber="3" x="-237.365" y="171.735" z="0.000" rz="135.00"/>
		<Component partNumber="511-500590" refDes="C319" circuitNumber="4" x="-101.225" y="171.735" z="0.000" rz="135.00"/>
		<Component partNumber="511-500590" refDes="C323" circuitNumber="1" x="-223.496" y="66.278" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C323" circuitNumber="2" x="-87.356" y="66.278" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C323" circuitNumber="3" x="-223.496" y="171.078" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C323" circuitNumber="4" x="-87.356" y="171.078" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C325" circuitNumber="1" x="-232.688" y="65.309" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C325" circuitNumber="2" x="-96.548" y="65.309" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C325" circuitNumber="3" x="-232.688" y="170.109" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C325" circuitNumber="4" x="-96.548" y="170.109" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C329" circuitNumber="1" x="-138.054" y="64.587" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C329" circuitNumber="2" x="-1.914" y="64.587" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C329" circuitNumber="3" x="-138.054" y="169.387" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C329" circuitNumber="4" x="-1.914" y="169.387" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C336" circuitNumber="1" x="-257.204" y="60.395" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C336" circuitNumber="2" x="-121.064" y="60.395" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C336" circuitNumber="3" x="-257.204" y="165.195" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C336" circuitNumber="4" x="-121.064" y="165.195" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C337" circuitNumber="1" x="-255.680" y="60.395" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C337" circuitNumber="2" x="-119.540" y="60.395" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C337" circuitNumber="3" x="-255.680" y="165.195" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C337" circuitNumber="4" x="-119.540" y="165.195" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C338" circuitNumber="1" x="-253.140" y="60.395" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C338" circuitNumber="2" x="-117.000" y="60.395" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C338" circuitNumber="3" x="-253.140" y="165.195" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C338" circuitNumber="4" x="-117.000" y="165.195" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C355" circuitNumber="1" x="-176.535" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C355" circuitNumber="2" x="-40.395" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C355" circuitNumber="3" x="-176.535" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C355" circuitNumber="4" x="-40.395" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C36" circuitNumber="1" x="-251.743" y="95.066" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C36" circuitNumber="2" x="-115.603" y="95.066" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C36" circuitNumber="3" x="-251.743" y="199.866" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C36" circuitNumber="4" x="-115.603" y="199.866" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C362" circuitNumber="1" x="-157.485" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C362" circuitNumber="2" x="-21.345" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C362" circuitNumber="3" x="-157.485" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C362" circuitNumber="4" x="-21.345" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C37" circuitNumber="1" x="-249.711" y="95.066" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C37" circuitNumber="2" x="-113.571" y="95.066" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C37" circuitNumber="3" x="-249.711" y="199.866" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C37" circuitNumber="4" x="-113.571" y="199.866" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C373" circuitNumber="1" x="-253.267" y="48.838" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C373" circuitNumber="2" x="-117.127" y="48.838" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C373" circuitNumber="3" x="-253.267" y="153.638" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C373" circuitNumber="4" x="-117.127" y="153.638" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C374" circuitNumber="1" x="-251.235" y="48.838" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C374" circuitNumber="2" x="-115.095" y="48.838" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C374" circuitNumber="3" x="-251.235" y="153.638" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C374" circuitNumber="4" x="-115.095" y="153.638" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C38" circuitNumber="1" x="-247.806" y="95.066" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C38" circuitNumber="2" x="-111.666" y="95.066" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C38" circuitNumber="3" x="-247.806" y="199.866" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C38" circuitNumber="4" x="-111.666" y="199.866" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C386" circuitNumber="1" x="-241.456" y="42.996" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C386" circuitNumber="2" x="-105.316" y="42.996" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C386" circuitNumber="3" x="-241.456" y="147.796" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C386" circuitNumber="4" x="-105.316" y="147.796" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C389" circuitNumber="1" x="-177.678" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C389" circuitNumber="2" x="-41.538" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C389" circuitNumber="3" x="-177.678" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C389" circuitNumber="4" x="-41.538" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C394" circuitNumber="1" x="-158.628" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C394" circuitNumber="2" x="-22.488" y="42.361" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C394" circuitNumber="3" x="-158.628" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C394" circuitNumber="4" x="-22.488" y="147.161" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C399" circuitNumber="1" x="-253.789" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C399" circuitNumber="2" x="-117.649" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C399" circuitNumber="3" x="-253.789" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C399" circuitNumber="4" x="-117.649" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C400" circuitNumber="1" x="-252.665" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C400" circuitNumber="2" x="-116.525" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C400" circuitNumber="3" x="-252.665" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C400" circuitNumber="4" x="-116.525" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C401" circuitNumber="1" x="-251.290" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C401" circuitNumber="2" x="-115.150" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C401" circuitNumber="3" x="-251.290" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C401" circuitNumber="4" x="-115.150" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C402" circuitNumber="1" x="-250.165" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C402" circuitNumber="2" x="-114.025" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C402" circuitNumber="3" x="-250.165" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C402" circuitNumber="4" x="-114.025" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C403" circuitNumber="1" x="-248.791" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C403" circuitNumber="2" x="-112.651" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C403" circuitNumber="3" x="-248.791" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C403" circuitNumber="4" x="-112.651" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C404" circuitNumber="1" x="-247.821" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C404" circuitNumber="2" x="-111.681" y="42.064" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C404" circuitNumber="3" x="-247.821" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C404" circuitNumber="4" x="-111.681" y="146.864" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C407" circuitNumber="1" x="-242.688" y="39.659" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C407" circuitNumber="2" x="-106.548" y="39.659" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C407" circuitNumber="3" x="-242.688" y="144.459" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C407" circuitNumber="4" x="-106.548" y="144.459" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C413" circuitNumber="1" x="-254.029" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C413" circuitNumber="2" x="-117.889" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C413" circuitNumber="3" x="-254.029" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C413" circuitNumber="4" x="-117.889" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C414" circuitNumber="1" x="-252.632" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C414" circuitNumber="2" x="-116.492" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C414" circuitNumber="3" x="-252.632" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C414" circuitNumber="4" x="-116.492" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C415" circuitNumber="1" x="-251.108" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C415" circuitNumber="2" x="-114.968" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C415" circuitNumber="3" x="-251.108" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C415" circuitNumber="4" x="-114.968" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C416" circuitNumber="1" x="-249.711" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C416" circuitNumber="2" x="-113.571" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C416" circuitNumber="3" x="-249.711" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C416" circuitNumber="4" x="-113.571" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C417" circuitNumber="1" x="-248.314" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C417" circuitNumber="2" x="-112.174" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C417" circuitNumber="3" x="-248.314" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C417" circuitNumber="4" x="-112.174" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C418" circuitNumber="1" x="-247.044" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C418" circuitNumber="2" x="-110.904" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C418" circuitNumber="3" x="-247.044" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C418" circuitNumber="4" x="-110.904" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C419" circuitNumber="1" x="-245.647" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C419" circuitNumber="2" x="-109.507" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C419" circuitNumber="3" x="-245.647" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C419" circuitNumber="4" x="-109.507" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C420" circuitNumber="1" x="-244.377" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C420" circuitNumber="2" x="-108.237" y="35.884" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C420" circuitNumber="3" x="-244.377" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C420" circuitNumber="4" x="-108.237" y="140.684" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C442" circuitNumber="1" x="-176.535" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C442" circuitNumber="2" x="-40.395" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C442" circuitNumber="3" x="-176.535" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C442" circuitNumber="4" x="-40.395" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C449" circuitNumber="1" x="-157.485" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C449" circuitNumber="2" x="-21.345" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C449" circuitNumber="3" x="-157.485" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C449" circuitNumber="4" x="-21.345" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C463" circuitNumber="1" x="-177.551" y="11.984" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C463" circuitNumber="2" x="-41.411" y="11.984" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C463" circuitNumber="3" x="-177.551" y="116.784" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C463" circuitNumber="4" x="-41.411" y="116.784" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C468" circuitNumber="1" x="-158.755" y="11.984" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C468" circuitNumber="2" x="-22.615" y="11.984" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C468" circuitNumber="3" x="-158.755" y="116.784" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C468" circuitNumber="4" x="-22.615" y="116.784" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C481" circuitNumber="1" x="-246.028" y="69.285" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C481" circuitNumber="2" x="-109.888" y="69.285" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C481" circuitNumber="3" x="-246.028" y="174.085" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C481" circuitNumber="4" x="-109.888" y="174.085" z="0.000" rz="270.00"/>
		<Component partNumber="511-500590" refDes="C58" circuitNumber="1" x="-176.535" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C58" circuitNumber="2" x="-40.395" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C58" circuitNumber="3" x="-176.535" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C58" circuitNumber="4" x="-40.395" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C65" circuitNumber="1" x="-157.485" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C65" circuitNumber="2" x="-21.345" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C65" circuitNumber="3" x="-157.485" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C65" circuitNumber="4" x="-21.345" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500590" refDes="C69" circuitNumber="1" x="-217.496" y="87.478" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C69" circuitNumber="2" x="-81.356" y="87.478" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C69" circuitNumber="3" x="-217.496" y="192.278" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C69" circuitNumber="4" x="-81.356" y="192.278" z="0.000" rz="0.00"/>
		<Component partNumber="511-500590" refDes="C76" circuitNumber="1" x="-220.296" y="86.278" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C76" circuitNumber="2" x="-84.156" y="86.278" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C76" circuitNumber="3" x="-220.296" y="191.078" z="0.000" rz="90.00"/>
		<Component partNumber="511-500590" refDes="C76" circuitNumber="4" x="-84.156" y="191.078" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C19" circuitNumber="1" x="-242.751" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C19" circuitNumber="2" x="-106.611" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C19" circuitNumber="3" x="-242.751" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C19" circuitNumber="4" x="-106.611" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C20" circuitNumber="1" x="-242.066" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C20" circuitNumber="2" x="-105.926" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C20" circuitNumber="3" x="-242.066" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C20" circuitNumber="4" x="-105.926" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C23" circuitNumber="1" x="-229.950" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C23" circuitNumber="2" x="-93.810" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C23" circuitNumber="3" x="-229.950" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C23" circuitNumber="4" x="-93.810" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C24" circuitNumber="1" x="-229.264" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C24" circuitNumber="2" x="-93.124" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C24" circuitNumber="3" x="-229.264" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C24" circuitNumber="4" x="-93.124" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C249" circuitNumber="1" x="-227.070" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500595" refDes="C249" circuitNumber="2" x="-90.930" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500595" refDes="C249" circuitNumber="3" x="-227.070" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500595" refDes="C249" circuitNumber="4" x="-90.930" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500595" refDes="C269" circuitNumber="1" x="-231.473" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500595" refDes="C269" circuitNumber="2" x="-95.333" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500595" refDes="C269" circuitNumber="3" x="-231.473" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500595" refDes="C269" circuitNumber="4" x="-95.333" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500595" refDes="C27" circuitNumber="1" x="-225.149" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C27" circuitNumber="2" x="-89.009" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C27" circuitNumber="3" x="-225.149" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C27" circuitNumber="4" x="-89.009" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C28" circuitNumber="1" x="-224.463" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C28" circuitNumber="2" x="-88.323" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C28" circuitNumber="3" x="-224.463" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C28" circuitNumber="4" x="-88.323" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C29" circuitNumber="1" x="-220.349" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C29" circuitNumber="2" x="-84.209" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C29" circuitNumber="3" x="-220.349" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C29" circuitNumber="4" x="-84.209" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C30" circuitNumber="1" x="-219.663" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C30" circuitNumber="2" x="-83.523" y="98.800" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C30" circuitNumber="3" x="-219.663" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C30" circuitNumber="4" x="-83.523" y="203.600" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C741" circuitNumber="1" x="-235.688" y="98.817" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C741" circuitNumber="2" x="-99.548" y="98.817" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C741" circuitNumber="3" x="-235.688" y="203.617" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C741" circuitNumber="4" x="-99.548" y="203.617" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C742" circuitNumber="1" x="-236.338" y="98.817" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C742" circuitNumber="2" x="-100.198" y="98.817" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C742" circuitNumber="3" x="-236.338" y="203.617" z="0.000" rz="90.00"/>
		<Component partNumber="511-500595" refDes="C742" circuitNumber="4" x="-100.198" y="203.617" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C1" circuitNumber="1" x="-256.938" y="104.509" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C1" circuitNumber="2" x="-120.798" y="104.509" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C1" circuitNumber="3" x="-256.938" y="209.309" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C1" circuitNumber="4" x="-120.798" y="209.309" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C102" circuitNumber="1" x="-253.902" y="83.382" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C102" circuitNumber="2" x="-117.762" y="83.382" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C102" circuitNumber="3" x="-253.902" y="188.182" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C102" circuitNumber="4" x="-117.762" y="188.182" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C103" circuitNumber="1" x="-249.965" y="83.382" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C103" circuitNumber="2" x="-113.825" y="83.382" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C103" circuitNumber="3" x="-249.965" y="188.182" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C103" circuitNumber="4" x="-113.825" y="188.182" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C105" circuitNumber="1" x="-153.040" y="84.083" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C105" circuitNumber="2" x="-16.900" y="84.083" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C105" circuitNumber="3" x="-153.040" y="188.883" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C105" circuitNumber="4" x="-16.900" y="188.883" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C110" circuitNumber="1" x="-233.838" y="83.011" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C110" circuitNumber="2" x="-97.698" y="83.011" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C110" circuitNumber="3" x="-233.838" y="187.811" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C110" circuitNumber="4" x="-97.698" y="187.811" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C114" circuitNumber="1" x="-227.070" y="82.531" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C114" circuitNumber="2" x="-90.930" y="82.531" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C114" circuitNumber="3" x="-227.070" y="187.331" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C114" circuitNumber="4" x="-90.930" y="187.331" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C115" circuitNumber="1" x="-225.769" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C115" circuitNumber="2" x="-89.629" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C115" circuitNumber="3" x="-225.769" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C115" circuitNumber="4" x="-89.629" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C116" circuitNumber="1" x="-224.469" y="82.531" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C116" circuitNumber="2" x="-88.329" y="82.531" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C116" circuitNumber="3" x="-224.469" y="187.331" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C116" circuitNumber="4" x="-88.329" y="187.331" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C117" circuitNumber="1" x="-232.771" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C117" circuitNumber="2" x="-96.631" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C117" circuitNumber="3" x="-232.771" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C117" circuitNumber="4" x="-96.631" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C118" circuitNumber="1" x="-231.471" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C118" circuitNumber="2" x="-95.331" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C118" circuitNumber="3" x="-231.471" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C118" circuitNumber="4" x="-95.331" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C119" circuitNumber="1" x="-230.170" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C119" circuitNumber="2" x="-94.030" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C119" circuitNumber="3" x="-230.170" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C119" circuitNumber="4" x="-94.030" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C120" circuitNumber="1" x="-228.870" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C120" circuitNumber="2" x="-92.730" y="82.530" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C120" circuitNumber="3" x="-228.870" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C120" circuitNumber="4" x="-92.730" y="187.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C121" circuitNumber="1" x="-235.096" y="81.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C121" circuitNumber="2" x="-98.956" y="81.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C121" circuitNumber="3" x="-235.096" y="186.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C121" circuitNumber="4" x="-98.956" y="186.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C124" circuitNumber="1" x="-227.070" y="81.881" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C124" circuitNumber="2" x="-90.930" y="81.881" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C124" circuitNumber="3" x="-227.070" y="186.681" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C124" circuitNumber="4" x="-90.930" y="186.681" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C125" circuitNumber="1" x="-225.769" y="81.881" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C125" circuitNumber="2" x="-89.629" y="81.881" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C125" circuitNumber="3" x="-225.769" y="186.681" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C125" circuitNumber="4" x="-89.629" y="186.681" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C126" circuitNumber="1" x="-224.469" y="81.881" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C126" circuitNumber="2" x="-88.329" y="81.881" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C126" circuitNumber="3" x="-224.469" y="186.681" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C126" circuitNumber="4" x="-88.329" y="186.681" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C127" circuitNumber="1" x="-232.771" y="81.880" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C127" circuitNumber="2" x="-96.631" y="81.880" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C127" circuitNumber="3" x="-232.771" y="186.680" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C127" circuitNumber="4" x="-96.631" y="186.680" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C128" circuitNumber="1" x="-231.471" y="81.880" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C128" circuitNumber="2" x="-95.331" y="81.880" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C128" circuitNumber="3" x="-231.471" y="186.680" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C128" circuitNumber="4" x="-95.331" y="186.680" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C129" circuitNumber="1" x="-230.170" y="81.880" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C129" circuitNumber="2" x="-94.030" y="81.880" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C129" circuitNumber="3" x="-230.170" y="186.680" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C129" circuitNumber="4" x="-94.030" y="186.680" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C130" circuitNumber="1" x="-228.870" y="81.880" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C130" circuitNumber="2" x="-92.730" y="81.880" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C130" circuitNumber="3" x="-228.870" y="186.680" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C130" circuitNumber="4" x="-92.730" y="186.680" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C132" circuitNumber="1" x="-234.228" y="81.559" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C132" circuitNumber="2" x="-98.088" y="81.559" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C132" circuitNumber="3" x="-234.228" y="186.359" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C132" circuitNumber="4" x="-98.088" y="186.359" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C133" circuitNumber="1" x="-233.638" y="81.559" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C133" circuitNumber="2" x="-97.498" y="81.559" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C133" circuitNumber="3" x="-233.638" y="186.359" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C133" circuitNumber="4" x="-97.498" y="186.359" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C136" circuitNumber="1" x="-225.770" y="81.230" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C136" circuitNumber="2" x="-89.630" y="81.230" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C136" circuitNumber="3" x="-225.770" y="186.030" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C136" circuitNumber="4" x="-89.630" y="186.030" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C138" circuitNumber="1" x="-232.771" y="81.230" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C138" circuitNumber="2" x="-96.631" y="81.230" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C138" circuitNumber="3" x="-232.771" y="186.030" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C138" circuitNumber="4" x="-96.631" y="186.030" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C139" circuitNumber="1" x="-231.471" y="81.229" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C139" circuitNumber="2" x="-95.331" y="81.229" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C139" circuitNumber="3" x="-231.471" y="186.029" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C139" circuitNumber="4" x="-95.331" y="186.029" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C140" circuitNumber="1" x="-223.494" y="80.905" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C140" circuitNumber="2" x="-87.354" y="80.905" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C140" circuitNumber="3" x="-223.494" y="185.705" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C140" circuitNumber="4" x="-87.354" y="185.705" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C142" circuitNumber="1" x="-218.696" y="80.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C142" circuitNumber="2" x="-82.556" y="80.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C142" circuitNumber="3" x="-218.696" y="185.478" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C142" circuitNumber="4" x="-82.556" y="185.478" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C143" circuitNumber="1" x="-222.462" y="80.588" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C143" circuitNumber="2" x="-86.322" y="80.588" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C143" circuitNumber="3" x="-222.462" y="185.388" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C143" circuitNumber="4" x="-86.322" y="185.388" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C144" circuitNumber="1" x="-227.070" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C144" circuitNumber="2" x="-90.930" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C144" circuitNumber="3" x="-227.070" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C144" circuitNumber="4" x="-90.930" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C147" circuitNumber="1" x="-234.072" y="80.579" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C147" circuitNumber="2" x="-97.932" y="80.579" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C147" circuitNumber="3" x="-234.072" y="185.379" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C147" circuitNumber="4" x="-97.932" y="185.379" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C15" circuitNumber="1" x="-237.646" y="102.159" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C15" circuitNumber="2" x="-101.506" y="102.159" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C15" circuitNumber="3" x="-237.646" y="206.959" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C15" circuitNumber="4" x="-101.506" y="206.959" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C150" circuitNumber="1" x="-230.171" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C150" circuitNumber="2" x="-94.031" y="80.580" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C150" circuitNumber="3" x="-230.171" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C150" circuitNumber="4" x="-94.031" y="185.380" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C155" circuitNumber="1" x="-223.169" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C155" circuitNumber="2" x="-87.029" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C155" circuitNumber="3" x="-223.169" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C155" circuitNumber="4" x="-87.029" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C157" circuitNumber="1" x="-232.771" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C157" circuitNumber="2" x="-96.631" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C157" circuitNumber="3" x="-232.771" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C157" circuitNumber="4" x="-96.631" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C158" circuitNumber="1" x="-231.471" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C158" circuitNumber="2" x="-95.331" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C158" circuitNumber="3" x="-231.471" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C158" circuitNumber="4" x="-95.331" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C159" circuitNumber="1" x="-230.171" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C159" circuitNumber="2" x="-94.031" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C159" circuitNumber="3" x="-230.171" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C159" circuitNumber="4" x="-94.031" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C160" circuitNumber="1" x="-228.871" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C160" circuitNumber="2" x="-92.731" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C160" circuitNumber="3" x="-228.871" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C160" circuitNumber="4" x="-92.731" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C161" circuitNumber="1" x="-225.770" y="79.929" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C161" circuitNumber="2" x="-89.630" y="79.929" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C161" circuitNumber="3" x="-225.770" y="184.729" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C161" circuitNumber="4" x="-89.630" y="184.729" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C164" circuitNumber="1" x="-232.771" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C164" circuitNumber="2" x="-96.631" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C164" circuitNumber="3" x="-232.771" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C164" circuitNumber="4" x="-96.631" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C165" circuitNumber="1" x="-227.070" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C165" circuitNumber="2" x="-90.930" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C165" circuitNumber="3" x="-227.070" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C165" circuitNumber="4" x="-90.930" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C166" circuitNumber="1" x="-225.769" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C166" circuitNumber="2" x="-89.629" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C166" circuitNumber="3" x="-225.769" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C166" circuitNumber="4" x="-89.629" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C169" circuitNumber="1" x="-230.171" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C169" circuitNumber="2" x="-94.031" y="79.280" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C169" circuitNumber="3" x="-230.171" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C169" circuitNumber="4" x="-94.031" y="184.080" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C17" circuitNumber="1" x="-218.823" y="101.401" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C17" circuitNumber="2" x="-82.683" y="101.401" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C17" circuitNumber="3" x="-218.823" y="206.201" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C17" circuitNumber="4" x="-82.683" y="206.201" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C176" circuitNumber="1" x="-227.070" y="78.629" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C176" circuitNumber="2" x="-90.930" y="78.629" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C176" circuitNumber="3" x="-227.070" y="183.429" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C176" circuitNumber="4" x="-90.930" y="183.429" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C179" circuitNumber="1" x="-232.771" y="78.630" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C179" circuitNumber="2" x="-96.631" y="78.630" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C179" circuitNumber="3" x="-232.771" y="183.430" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C179" circuitNumber="4" x="-96.631" y="183.430" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C180" circuitNumber="1" x="-231.471" y="78.630" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C180" circuitNumber="2" x="-95.331" y="78.630" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C180" circuitNumber="3" x="-231.471" y="183.430" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C180" circuitNumber="4" x="-95.331" y="183.430" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C182" circuitNumber="1" x="-228.871" y="78.630" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C182" circuitNumber="2" x="-92.731" y="78.630" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C182" circuitNumber="3" x="-228.871" y="183.430" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C182" circuitNumber="4" x="-92.731" y="183.430" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C185" circuitNumber="1" x="-225.769" y="77.979" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C185" circuitNumber="2" x="-89.629" y="77.979" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C185" circuitNumber="3" x="-225.769" y="182.779" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C185" circuitNumber="4" x="-89.629" y="182.779" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C186" circuitNumber="1" x="-224.469" y="77.979" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C186" circuitNumber="2" x="-88.329" y="77.979" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C186" circuitNumber="3" x="-224.469" y="182.779" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C186" circuitNumber="4" x="-88.329" y="182.779" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C188" circuitNumber="1" x="-230.171" y="77.980" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C188" circuitNumber="2" x="-94.031" y="77.980" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C188" circuitNumber="3" x="-230.171" y="182.780" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C188" circuitNumber="4" x="-94.031" y="182.780" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C189" circuitNumber="1" x="-228.871" y="77.980" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C189" circuitNumber="2" x="-92.731" y="77.980" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C189" circuitNumber="3" x="-228.871" y="182.780" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C189" circuitNumber="4" x="-92.731" y="182.780" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C190" circuitNumber="1" x="-222.844" y="77.806" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C190" circuitNumber="2" x="-86.704" y="77.806" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C190" circuitNumber="3" x="-222.844" y="182.606" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C190" circuitNumber="4" x="-86.704" y="182.606" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C193" circuitNumber="1" x="-233.748" y="77.670" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C193" circuitNumber="2" x="-97.608" y="77.670" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C193" circuitNumber="3" x="-233.748" y="182.470" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C193" circuitNumber="4" x="-97.608" y="182.470" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C194" circuitNumber="1" x="-223.494" y="77.654" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C194" circuitNumber="2" x="-87.354" y="77.654" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C194" circuitNumber="3" x="-223.494" y="182.454" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C194" circuitNumber="4" x="-87.354" y="182.454" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C197" circuitNumber="1" x="-231.471" y="77.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C197" circuitNumber="2" x="-95.331" y="77.330" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C197" circuitNumber="3" x="-231.471" y="182.130" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C197" circuitNumber="4" x="-95.331" y="182.130" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C199" circuitNumber="1" x="-227.070" y="77.329" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C199" circuitNumber="2" x="-90.930" y="77.329" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C199" circuitNumber="3" x="-227.070" y="182.129" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C199" circuitNumber="4" x="-90.930" y="182.129" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C202" circuitNumber="1" x="-239.088" y="76.859" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C202" circuitNumber="2" x="-102.948" y="76.859" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C202" circuitNumber="3" x="-239.088" y="181.659" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C202" circuitNumber="4" x="-102.948" y="181.659" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C203" circuitNumber="1" x="-153.040" y="77.605" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C203" circuitNumber="2" x="-16.900" y="77.605" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C203" circuitNumber="3" x="-153.040" y="182.405" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C203" circuitNumber="4" x="-16.900" y="182.405" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C204" circuitNumber="1" x="-233.438" y="76.729" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C204" circuitNumber="2" x="-97.298" y="76.729" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C204" circuitNumber="3" x="-233.438" y="181.529" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C204" circuitNumber="4" x="-97.298" y="181.529" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C208" circuitNumber="1" x="-230.172" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C208" circuitNumber="2" x="-94.032" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C208" circuitNumber="3" x="-230.172" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C208" circuitNumber="4" x="-94.032" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C210" circuitNumber="1" x="-227.070" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C210" circuitNumber="2" x="-90.930" y="76.177" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C210" circuitNumber="3" x="-227.070" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C210" circuitNumber="4" x="-90.930" y="180.977" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C214" circuitNumber="1" x="-259.998" y="77.216" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C214" circuitNumber="2" x="-123.858" y="77.216" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C214" circuitNumber="3" x="-259.998" y="182.016" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C214" circuitNumber="4" x="-123.858" y="182.016" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C216" circuitNumber="1" x="-232.773" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C216" circuitNumber="2" x="-96.633" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C216" circuitNumber="3" x="-232.773" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C216" circuitNumber="4" x="-96.633" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C217" circuitNumber="1" x="-231.473" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C217" circuitNumber="2" x="-95.333" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C217" circuitNumber="3" x="-231.473" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C217" circuitNumber="4" x="-95.333" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C218" circuitNumber="1" x="-230.172" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C218" circuitNumber="2" x="-94.032" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C218" circuitNumber="3" x="-230.172" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C218" circuitNumber="4" x="-94.032" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C219" circuitNumber="1" x="-228.872" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C219" circuitNumber="2" x="-92.732" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C219" circuitNumber="3" x="-228.872" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C219" circuitNumber="4" x="-92.732" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C221" circuitNumber="1" x="-225.770" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C221" circuitNumber="2" x="-89.630" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C221" circuitNumber="3" x="-225.770" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C221" circuitNumber="4" x="-89.630" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C222" circuitNumber="1" x="-223.169" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C222" circuitNumber="2" x="-87.029" y="75.527" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C222" circuitNumber="3" x="-223.169" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C222" circuitNumber="4" x="-87.029" y="180.327" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C223" circuitNumber="1" x="-218.296" y="75.478" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C223" circuitNumber="2" x="-82.156" y="75.478" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C223" circuitNumber="3" x="-218.296" y="180.278" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C223" circuitNumber="4" x="-82.156" y="180.278" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C224" circuitNumber="1" x="-233.748" y="75.202" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C224" circuitNumber="2" x="-97.608" y="75.202" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C224" circuitNumber="3" x="-233.748" y="180.002" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C224" circuitNumber="4" x="-97.608" y="180.002" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C227" circuitNumber="1" x="-232.773" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C227" circuitNumber="2" x="-96.633" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C227" circuitNumber="3" x="-232.773" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C227" circuitNumber="4" x="-96.633" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C228" circuitNumber="1" x="-231.473" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C228" circuitNumber="2" x="-95.333" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C228" circuitNumber="3" x="-231.473" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C228" circuitNumber="4" x="-95.333" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C229" circuitNumber="1" x="-230.172" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C229" circuitNumber="2" x="-94.032" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C229" circuitNumber="3" x="-230.172" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C229" circuitNumber="4" x="-94.032" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C231" circuitNumber="1" x="-227.070" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C231" circuitNumber="2" x="-90.930" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C231" circuitNumber="3" x="-227.070" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C231" circuitNumber="4" x="-90.930" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C232" circuitNumber="1" x="-225.769" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C232" circuitNumber="2" x="-89.629" y="74.877" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C232" circuitNumber="3" x="-225.769" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C232" circuitNumber="4" x="-89.629" y="179.677" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C234" circuitNumber="1" x="-230.172" y="74.228" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C234" circuitNumber="2" x="-94.032" y="74.228" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C234" circuitNumber="3" x="-230.172" y="179.028" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C234" circuitNumber="4" x="-94.032" y="179.028" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C235" circuitNumber="1" x="-232.773" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C235" circuitNumber="2" x="-96.633" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C235" circuitNumber="3" x="-232.773" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C235" circuitNumber="4" x="-96.633" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C236" circuitNumber="1" x="-231.473" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C236" circuitNumber="2" x="-95.333" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C236" circuitNumber="3" x="-231.473" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C236" circuitNumber="4" x="-95.333" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C237" circuitNumber="1" x="-228.872" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C237" circuitNumber="2" x="-92.732" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C237" circuitNumber="3" x="-228.872" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C237" circuitNumber="4" x="-92.732" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C238" circuitNumber="1" x="-225.770" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C238" circuitNumber="2" x="-89.630" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C238" circuitNumber="3" x="-225.770" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C238" circuitNumber="4" x="-89.630" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C240" circuitNumber="1" x="-223.169" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C240" circuitNumber="2" x="-87.029" y="74.227" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C240" circuitNumber="3" x="-223.169" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C240" circuitNumber="4" x="-87.029" y="179.027" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C241" circuitNumber="1" x="-239.265" y="74.435" z="0.000" rz="135.00"/>
		<Component partNumber="511-500615" refDes="C241" circuitNumber="2" x="-103.125" y="74.435" z="0.000" rz="135.00"/>
		<Component partNumber="511-500615" refDes="C241" circuitNumber="3" x="-239.265" y="179.235" z="0.000" rz="135.00"/>
		<Component partNumber="511-500615" refDes="C241" circuitNumber="4" x="-103.125" y="179.235" z="0.000" rz="135.00"/>
		<Component partNumber="511-500615" refDes="C245" circuitNumber="1" x="-222.368" y="73.218" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C245" circuitNumber="2" x="-86.228" y="73.218" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C245" circuitNumber="3" x="-222.368" y="178.018" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C245" circuitNumber="4" x="-86.228" y="178.018" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C250" circuitNumber="1" x="-225.769" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C250" circuitNumber="2" x="-89.629" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C250" circuitNumber="3" x="-225.769" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C250" circuitNumber="4" x="-89.629" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C252" circuitNumber="1" x="-223.183" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C252" circuitNumber="2" x="-87.043" y="73.577" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C252" circuitNumber="3" x="-223.183" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C252" circuitNumber="4" x="-87.043" y="178.377" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C271" circuitNumber="1" x="-228.872" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C271" circuitNumber="2" x="-92.732" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C271" circuitNumber="3" x="-228.872" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C271" circuitNumber="4" x="-92.732" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C273" circuitNumber="1" x="-225.770" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C273" circuitNumber="2" x="-89.630" y="72.926" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C273" circuitNumber="3" x="-225.770" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C273" circuitNumber="4" x="-89.630" y="177.726" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C274" circuitNumber="1" x="-233.848" y="72.601" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C274" circuitNumber="2" x="-97.708" y="72.601" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C274" circuitNumber="3" x="-233.848" y="177.401" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C274" circuitNumber="4" x="-97.708" y="177.401" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C277" circuitNumber="1" x="-232.773" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C277" circuitNumber="2" x="-96.633" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C277" circuitNumber="3" x="-232.773" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C277" circuitNumber="4" x="-96.633" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C279" circuitNumber="1" x="-230.172" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C279" circuitNumber="2" x="-94.032" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C279" circuitNumber="3" x="-230.172" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C279" circuitNumber="4" x="-94.032" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C281" circuitNumber="1" x="-227.070" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C281" circuitNumber="2" x="-90.930" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C281" circuitNumber="3" x="-227.070" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C281" circuitNumber="4" x="-90.930" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C283" circuitNumber="1" x="-224.469" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C283" circuitNumber="2" x="-88.329" y="72.276" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C283" circuitNumber="3" x="-224.469" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C283" circuitNumber="4" x="-88.329" y="177.076" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C288" circuitNumber="1" x="-255.934" y="71.825" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C288" circuitNumber="2" x="-119.794" y="71.825" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C288" circuitNumber="3" x="-255.934" y="176.625" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C288" circuitNumber="4" x="-119.794" y="176.625" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C289" circuitNumber="1" x="-253.902" y="71.825" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C289" circuitNumber="2" x="-117.762" y="71.825" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C289" circuitNumber="3" x="-253.902" y="176.625" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C289" circuitNumber="4" x="-117.762" y="176.625" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C290" circuitNumber="1" x="-249.965" y="71.825" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C290" circuitNumber="2" x="-113.825" y="71.825" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C290" circuitNumber="3" x="-249.965" y="176.625" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C290" circuitNumber="4" x="-113.825" y="176.625" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C294" circuitNumber="1" x="-232.121" y="71.627" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C294" circuitNumber="2" x="-95.981" y="71.627" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C294" circuitNumber="3" x="-232.121" y="176.427" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C294" circuitNumber="4" x="-95.981" y="176.427" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C295" circuitNumber="1" x="-227.070" y="71.626" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C295" circuitNumber="2" x="-90.930" y="71.626" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C295" circuitNumber="3" x="-227.070" y="176.426" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C295" circuitNumber="4" x="-90.930" y="176.426" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C296" circuitNumber="1" x="-223.165" y="71.627" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C296" circuitNumber="2" x="-87.025" y="71.627" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C296" circuitNumber="3" x="-223.165" y="176.427" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C296" circuitNumber="4" x="-87.025" y="176.427" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C299" circuitNumber="1" x="-219.262" y="71.232" z="0.000" rz="315.00"/>
		<Component partNumber="511-500615" refDes="C299" circuitNumber="2" x="-83.122" y="71.232" z="0.000" rz="315.00"/>
		<Component partNumber="511-500615" refDes="C299" circuitNumber="3" x="-219.262" y="176.032" z="0.000" rz="315.00"/>
		<Component partNumber="511-500615" refDes="C299" circuitNumber="4" x="-83.122" y="176.032" z="0.000" rz="315.00"/>
		<Component partNumber="511-500615" refDes="C306" circuitNumber="1" x="-237.896" y="69.478" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C306" circuitNumber="2" x="-101.756" y="69.478" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C306" circuitNumber="3" x="-237.896" y="174.278" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C306" circuitNumber="4" x="-101.756" y="174.278" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C320" circuitNumber="1" x="-234.915" y="67.232" z="0.000" rz="225.00"/>
		<Component partNumber="511-500615" refDes="C320" circuitNumber="2" x="-98.775" y="67.232" z="0.000" rz="225.00"/>
		<Component partNumber="511-500615" refDes="C320" circuitNumber="3" x="-234.915" y="172.032" z="0.000" rz="225.00"/>
		<Component partNumber="511-500615" refDes="C320" circuitNumber="4" x="-98.775" y="172.032" z="0.000" rz="225.00"/>
		<Component partNumber="511-500615" refDes="C321" circuitNumber="1" x="-225.096" y="67.078" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C321" circuitNumber="2" x="-88.956" y="67.078" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C321" circuitNumber="3" x="-225.096" y="171.878" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C321" circuitNumber="4" x="-88.956" y="171.878" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C322" circuitNumber="1" x="-218.696" y="66.278" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C322" circuitNumber="2" x="-82.556" y="66.278" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C322" circuitNumber="3" x="-218.696" y="171.078" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C322" circuitNumber="4" x="-82.556" y="171.078" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C326" circuitNumber="1" x="-237.496" y="65.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C326" circuitNumber="2" x="-101.356" y="65.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C326" circuitNumber="3" x="-237.496" y="170.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C326" circuitNumber="4" x="-101.356" y="170.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C330" circuitNumber="1" x="-153.040" y="64.333" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C330" circuitNumber="2" x="-16.900" y="64.333" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C330" circuitNumber="3" x="-153.040" y="169.133" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C330" circuitNumber="4" x="-16.900" y="169.133" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C335" circuitNumber="1" x="-243.492" y="64.205" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C335" circuitNumber="2" x="-107.352" y="64.205" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C335" circuitNumber="3" x="-243.492" y="169.005" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C335" circuitNumber="4" x="-107.352" y="169.005" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C339" circuitNumber="1" x="-250.219" y="60.395" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C339" circuitNumber="2" x="-114.079" y="60.395" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C339" circuitNumber="3" x="-250.219" y="165.195" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C339" circuitNumber="4" x="-114.079" y="165.195" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C341" circuitNumber="1" x="-245.266" y="60.395" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C341" circuitNumber="2" x="-109.126" y="60.395" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C341" circuitNumber="3" x="-245.266" y="165.195" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C341" circuitNumber="4" x="-109.126" y="165.195" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C35" circuitNumber="1" x="-253.775" y="95.066" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C35" circuitNumber="2" x="-117.635" y="95.066" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C35" circuitNumber="3" x="-253.775" y="199.866" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C35" circuitNumber="4" x="-117.635" y="199.866" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C352" circuitNumber="1" x="-184.409" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C352" circuitNumber="2" x="-48.269" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C352" circuitNumber="3" x="-184.409" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C352" circuitNumber="4" x="-48.269" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C354" circuitNumber="1" x="-177.805" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C354" circuitNumber="2" x="-41.665" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C354" circuitNumber="3" x="-177.805" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C354" circuitNumber="4" x="-41.665" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C359" circuitNumber="1" x="-165.359" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C359" circuitNumber="2" x="-29.219" y="57.855" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C359" circuitNumber="3" x="-165.359" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C359" circuitNumber="4" x="-29.219" y="162.655" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C361" circuitNumber="1" x="-158.755" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C361" circuitNumber="2" x="-22.615" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C361" circuitNumber="3" x="-158.755" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C361" circuitNumber="4" x="-22.615" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C364" circuitNumber="1" x="-153.040" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C364" circuitNumber="2" x="-16.900" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C364" circuitNumber="3" x="-153.040" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C364" circuitNumber="4" x="-16.900" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C368" circuitNumber="1" x="-259.155" y="56.561" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C368" circuitNumber="2" x="-123.015" y="56.561" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C368" circuitNumber="3" x="-259.155" y="161.361" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C368" circuitNumber="4" x="-123.015" y="161.361" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C379" circuitNumber="1" x="-226.089" y="46.934" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C379" circuitNumber="2" x="-89.949" y="46.934" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C379" circuitNumber="3" x="-226.089" y="151.734" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C379" circuitNumber="4" x="-89.949" y="151.734" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C405" circuitNumber="1" x="-226.089" y="40.329" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C405" circuitNumber="2" x="-89.949" y="40.329" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C405" circuitNumber="3" x="-226.089" y="145.129" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C405" circuitNumber="4" x="-89.949" y="145.129" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C423" circuitNumber="1" x="-231.042" y="33.725" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C423" circuitNumber="2" x="-94.902" y="33.725" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C423" circuitNumber="3" x="-231.042" y="138.525" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C423" circuitNumber="4" x="-94.902" y="138.525" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C424" circuitNumber="1" x="-239.678" y="33.217" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C424" circuitNumber="2" x="-103.538" y="33.217" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C424" circuitNumber="3" x="-239.678" y="138.017" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C424" circuitNumber="4" x="-103.538" y="138.017" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C425" circuitNumber="1" x="-253.902" y="32.913" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C425" circuitNumber="2" x="-117.762" y="32.913" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C425" circuitNumber="3" x="-253.902" y="137.713" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C425" circuitNumber="4" x="-117.762" y="137.713" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C435" circuitNumber="1" x="-237.011" y="28.011" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C435" circuitNumber="2" x="-100.871" y="28.011" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C435" circuitNumber="3" x="-237.011" y="132.811" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C435" circuitNumber="4" x="-100.871" y="132.811" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C439" circuitNumber="1" x="-184.409" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C439" circuitNumber="2" x="-48.269" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C439" circuitNumber="3" x="-184.409" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C439" circuitNumber="4" x="-48.269" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C44" circuitNumber="1" x="-214.024" y="94.431" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C44" circuitNumber="2" x="-77.884" y="94.431" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C44" circuitNumber="3" x="-214.024" y="199.231" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C44" circuitNumber="4" x="-77.884" y="199.231" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C441" circuitNumber="1" x="-177.805" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C441" circuitNumber="2" x="-41.665" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C441" circuitNumber="3" x="-177.805" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C441" circuitNumber="4" x="-41.665" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C446" circuitNumber="1" x="-165.359" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C446" circuitNumber="2" x="-29.219" y="26.867" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C446" circuitNumber="3" x="-165.359" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C446" circuitNumber="4" x="-29.219" y="131.667" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C448" circuitNumber="1" x="-158.755" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C448" circuitNumber="2" x="-22.615" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C448" circuitNumber="3" x="-158.755" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C448" circuitNumber="4" x="-22.615" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C454" circuitNumber="1" x="-229.518" y="19.628" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C454" circuitNumber="2" x="-93.378" y="19.628" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C454" circuitNumber="3" x="-229.518" y="124.428" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C454" circuitNumber="4" x="-93.378" y="124.428" z="0.000" rz="270.00"/>
		<Component partNumber="511-500615" refDes="C482" circuitNumber="1" x="-255.738" y="95.059" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C482" circuitNumber="2" x="-119.598" y="95.059" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C482" circuitNumber="3" x="-255.738" y="199.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C482" circuitNumber="4" x="-119.598" y="199.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C55" circuitNumber="1" x="-184.409" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C55" circuitNumber="2" x="-48.269" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C55" circuitNumber="3" x="-184.409" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C55" circuitNumber="4" x="-48.269" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C57" circuitNumber="1" x="-177.805" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C57" circuitNumber="2" x="-41.665" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C57" circuitNumber="3" x="-177.805" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C57" circuitNumber="4" x="-41.665" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C62" circuitNumber="1" x="-165.359" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C62" circuitNumber="2" x="-29.219" y="88.843" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C62" circuitNumber="3" x="-165.359" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C62" circuitNumber="4" x="-29.219" y="193.643" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C64" circuitNumber="1" x="-158.755" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C64" circuitNumber="2" x="-22.615" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C64" circuitNumber="3" x="-158.755" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C64" circuitNumber="4" x="-22.615" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C71" circuitNumber="1" x="-241.096" y="86.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C71" circuitNumber="2" x="-104.956" y="86.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C71" circuitNumber="3" x="-241.096" y="191.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C71" circuitNumber="4" x="-104.956" y="191.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C72" circuitNumber="1" x="-231.096" y="86.628" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C72" circuitNumber="2" x="-94.956" y="86.628" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C72" circuitNumber="3" x="-231.096" y="191.428" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C72" circuitNumber="4" x="-94.956" y="191.428" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C73" circuitNumber="1" x="-229.496" y="86.628" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C73" circuitNumber="2" x="-93.356" y="86.628" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C73" circuitNumber="3" x="-229.496" y="191.428" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C73" circuitNumber="4" x="-93.356" y="191.428" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C738" circuitNumber="1" x="-264.538" y="68.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C738" circuitNumber="2" x="-128.398" y="68.859" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C738" circuitNumber="3" x="-264.538" y="173.659" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C738" circuitNumber="4" x="-128.398" y="173.659" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C74" circuitNumber="1" x="-226.296" y="86.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C74" circuitNumber="2" x="-90.156" y="86.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C74" circuitNumber="3" x="-226.296" y="191.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C74" circuitNumber="4" x="-90.156" y="191.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C78" circuitNumber="1" x="-232.696" y="85.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C78" circuitNumber="2" x="-96.556" y="85.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C78" circuitNumber="3" x="-232.696" y="190.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C78" circuitNumber="4" x="-96.556" y="190.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C79" circuitNumber="1" x="-231.096" y="85.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C79" circuitNumber="2" x="-94.956" y="85.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C79" circuitNumber="3" x="-231.096" y="190.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C79" circuitNumber="4" x="-94.956" y="190.678" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C80" circuitNumber="1" x="-229.496" y="85.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C80" circuitNumber="2" x="-93.356" y="85.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C80" circuitNumber="3" x="-229.496" y="190.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C80" circuitNumber="4" x="-93.356" y="190.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C81" circuitNumber="1" x="-226.296" y="85.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C81" circuitNumber="2" x="-90.156" y="85.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C81" circuitNumber="3" x="-226.296" y="190.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C81" circuitNumber="4" x="-90.156" y="190.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500615" refDes="C83" circuitNumber="1" x="-218.296" y="85.078" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C83" circuitNumber="2" x="-82.156" y="85.078" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C83" circuitNumber="3" x="-218.296" y="189.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C83" circuitNumber="4" x="-82.156" y="189.878" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C91" circuitNumber="1" x="-230.696" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C91" circuitNumber="2" x="-94.556" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C91" circuitNumber="3" x="-230.696" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C91" circuitNumber="4" x="-94.556" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C92" circuitNumber="1" x="-226.696" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C92" circuitNumber="2" x="-90.556" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C92" circuitNumber="3" x="-226.696" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C92" circuitNumber="4" x="-90.556" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C93" circuitNumber="1" x="-222.696" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C93" circuitNumber="2" x="-86.556" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C93" circuitNumber="3" x="-222.696" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C93" circuitNumber="4" x="-86.556" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C95" circuitNumber="1" x="-236.296" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C95" circuitNumber="2" x="-100.156" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C95" circuitNumber="3" x="-236.296" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C95" circuitNumber="4" x="-100.156" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C96" circuitNumber="1" x="-233.896" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C96" circuitNumber="2" x="-97.756" y="83.878" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C96" circuitNumber="3" x="-233.896" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C96" circuitNumber="4" x="-97.756" y="188.678" z="0.000" rz="90.00"/>
		<Component partNumber="511-500615" refDes="C97" circuitNumber="1" x="-264.316" y="84.836" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C97" circuitNumber="2" x="-128.176" y="84.836" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C97" circuitNumber="3" x="-264.316" y="189.636" z="0.000" rz="0.00"/>
		<Component partNumber="511-500615" refDes="C97" circuitNumber="4" x="-128.176" y="189.636" z="0.000" rz="0.00"/>
		<Component partNumber="511-500616" refDes="C12" circuitNumber="1" x="-248.949" y="102.941" z="0.000" rz="90.00"/>
		<Component partNumber="511-500616" refDes="C12" circuitNumber="2" x="-112.809" y="102.941" z="0.000" rz="90.00"/>
		<Component partNumber="511-500616" refDes="C12" circuitNumber="3" x="-248.949" y="207.741" z="0.000" rz="90.00"/>
		<Component partNumber="511-500616" refDes="C12" circuitNumber="4" x="-112.809" y="207.741" z="0.000" rz="90.00"/>
		<Component partNumber="511-500616" refDes="C453" circuitNumber="1" x="-240.588" y="21.359" z="0.000" rz="0.00"/>
		<Component partNumber="511-500616" refDes="C453" circuitNumber="2" x="-104.448" y="21.359" z="0.000" rz="0.00"/>
		<Component partNumber="511-500616" refDes="C453" circuitNumber="3" x="-240.588" y="126.159" z="0.000" rz="0.00"/>
		<Component partNumber="511-500616" refDes="C453" circuitNumber="4" x="-104.448" y="126.159" z="0.000" rz="0.00"/>
		<Component partNumber="511-500616" refDes="C75" circuitNumber="1" x="-223.096" y="86.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500616" refDes="C75" circuitNumber="2" x="-86.956" y="86.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500616" refDes="C75" circuitNumber="3" x="-223.096" y="191.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500616" refDes="C75" circuitNumber="4" x="-86.956" y="191.478" z="0.000" rz="180.00"/>
		<Component partNumber="511-500646" refDes="C172" circuitNumber="1" x="-264.316" y="80.137" z="0.000" rz="0.00"/>
		<Component partNumber="511-500646" refDes="C172" circuitNumber="2" x="-128.176" y="80.137" z="0.000" rz="0.00"/>
		<Component partNumber="511-500646" refDes="C172" circuitNumber="3" x="-264.316" y="184.937" z="0.000" rz="0.00"/>
		<Component partNumber="511-500646" refDes="C172" circuitNumber="4" x="-128.176" y="184.937" z="0.000" rz="0.00"/>
		<Component partNumber="511-500646" refDes="C743" circuitNumber="1" x="-245.838" y="101.559" z="0.000" rz="270.00"/>
		<Component partNumber="511-500646" refDes="C743" circuitNumber="2" x="-109.698" y="101.559" z="0.000" rz="270.00"/>
		<Component partNumber="511-500646" refDes="C743" circuitNumber="3" x="-245.838" y="206.359" z="0.000" rz="270.00"/>
		<Component partNumber="511-500646" refDes="C743" circuitNumber="4" x="-109.698" y="206.359" z="0.000" rz="270.00"/>
		<Component partNumber="511-500831" refDes="C382" circuitNumber="1" x="-243.996" y="43.747" z="0.000" rz="90.00"/>
		<Component partNumber="511-500831" refDes="C382" circuitNumber="2" x="-107.856" y="43.747" z="0.000" rz="90.00"/>
		<Component partNumber="511-500831" refDes="C382" circuitNumber="3" x="-243.996" y="148.547" z="0.000" rz="90.00"/>
		<Component partNumber="511-500831" refDes="C382" circuitNumber="4" x="-107.856" y="148.547" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C436" circuitNumber="1" x="-235.741" y="28.010" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C436" circuitNumber="2" x="-99.601" y="28.010" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C436" circuitNumber="3" x="-235.741" y="132.810" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C436" circuitNumber="4" x="-99.601" y="132.810" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C456" circuitNumber="1" x="-246.409" y="13.405" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C456" circuitNumber="2" x="-110.269" y="13.405" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C456" circuitNumber="3" x="-246.409" y="118.205" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C456" circuitNumber="4" x="-110.269" y="118.205" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C457" circuitNumber="1" x="-244.812" y="13.405" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C457" circuitNumber="2" x="-108.672" y="13.405" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C457" circuitNumber="3" x="-244.812" y="118.205" z="0.000" rz="90.00"/>
		<Component partNumber="511-500866" refDes="C457" circuitNumber="4" x="-108.672" y="118.205" z="0.000" rz="90.00"/>
		<Component partNumber="511-500892" refDes="C153" circuitNumber="1" x="-234.072" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C153" circuitNumber="2" x="-97.932" y="79.930" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C153" circuitNumber="3" x="-234.072" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C153" circuitNumber="4" x="-97.932" y="184.730" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C298" circuitNumber="1" x="-232.968" y="71.459" z="0.000" rz="270.00"/>
		<Component partNumber="511-500892" refDes="C298" circuitNumber="2" x="-96.828" y="71.459" z="0.000" rz="270.00"/>
		<Component partNumber="511-500892" refDes="C298" circuitNumber="3" x="-232.968" y="176.259" z="0.000" rz="270.00"/>
		<Component partNumber="511-500892" refDes="C298" circuitNumber="4" x="-96.828" y="176.259" z="0.000" rz="270.00"/>
		<Component partNumber="511-500892" refDes="C313" circuitNumber="1" x="-239.096" y="68.278" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C313" circuitNumber="2" x="-102.956" y="68.278" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C313" circuitNumber="3" x="-239.096" y="173.078" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C313" circuitNumber="4" x="-102.956" y="173.078" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C41" circuitNumber="1" x="-214.024" y="92.145" z="0.000" rz="0.00"/>
		<Component partNumber="511-500892" refDes="C41" circuitNumber="2" x="-77.884" y="92.145" z="0.000" rz="0.00"/>
		<Component partNumber="511-500892" refDes="C41" circuitNumber="3" x="-214.024" y="196.945" z="0.000" rz="0.00"/>
		<Component partNumber="511-500892" refDes="C41" circuitNumber="4" x="-77.884" y="196.945" z="0.000" rz="0.00"/>
		<Component partNumber="511-500892" refDes="C77" circuitNumber="1" x="-241.096" y="85.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C77" circuitNumber="2" x="-104.956" y="85.878" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C77" circuitNumber="3" x="-241.096" y="190.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500892" refDes="C77" circuitNumber="4" x="-104.956" y="190.678" z="0.000" rz="180.00"/>
		<Component partNumber="511-500893" refDes="C16" circuitNumber="1" x="-234.090" y="102.159" z="0.000" rz="0.00"/>
		<Component partNumber="511-500893" refDes="C16" circuitNumber="2" x="-97.950" y="102.159" z="0.000" rz="0.00"/>
		<Component partNumber="511-500893" refDes="C16" circuitNumber="3" x="-234.090" y="206.959" z="0.000" rz="0.00"/>
		<Component partNumber="511-500893" refDes="C16" circuitNumber="4" x="-97.950" y="206.959" z="0.000" rz="0.00"/>
		<Component partNumber="511-500896" refDes="C85" circuitNumber="1" x="-238.281" y="85.029" z="0.000" rz="90.00"/>
		<Component partNumber="511-500896" refDes="C85" circuitNumber="2" x="-102.141" y="85.029" z="0.000" rz="90.00"/>
		<Component partNumber="511-500896" refDes="C85" circuitNumber="3" x="-238.281" y="189.829" z="0.000" rz="90.00"/>
		<Component partNumber="511-500896" refDes="C85" circuitNumber="4" x="-102.141" y="189.829" z="0.000" rz="90.00"/>
		<Component partNumber="511-500896" refDes="C94" circuitNumber="1" x="-237.646" y="85.029" z="0.000" rz="90.00"/>
		<Component partNumber="511-500896" refDes="C94" circuitNumber="2" x="-101.506" y="85.029" z="0.000" rz="90.00"/>
		<Component partNumber="511-500896" refDes="C94" circuitNumber="3" x="-237.646" y="189.829" z="0.000" rz="90.00"/>
		<Component partNumber="511-500896" refDes="C94" circuitNumber="4" x="-101.506" y="189.829" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C292" circuitNumber="1" x="-245.688" y="72.279" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C292" circuitNumber="2" x="-109.548" y="72.279" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C292" circuitNumber="3" x="-245.688" y="177.079" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C292" circuitNumber="4" x="-109.548" y="177.079" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C311" circuitNumber="1" x="-240.696" y="69.076" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C311" circuitNumber="2" x="-104.556" y="69.076" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C311" circuitNumber="3" x="-240.696" y="173.876" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C311" circuitNumber="4" x="-104.556" y="173.876" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C324" circuitNumber="1" x="-261.868" y="69.015" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C324" circuitNumber="2" x="-125.728" y="69.015" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C324" circuitNumber="3" x="-261.868" y="173.815" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C324" circuitNumber="4" x="-125.728" y="173.815" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C332" circuitNumber="1" x="-261.868" y="66.967" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C332" circuitNumber="2" x="-125.728" y="66.967" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C332" circuitNumber="3" x="-261.868" y="171.767" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C332" circuitNumber="4" x="-125.728" y="171.767" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C334" circuitNumber="1" x="-261.868" y="64.919" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C334" circuitNumber="2" x="-125.728" y="64.919" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C334" circuitNumber="3" x="-261.868" y="169.719" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C334" circuitNumber="4" x="-125.728" y="169.719" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C430" circuitNumber="1" x="-238.413" y="30.109" z="0.000" rz="0.00"/>
		<Component partNumber="511-500917" refDes="C430" circuitNumber="2" x="-102.273" y="30.109" z="0.000" rz="0.00"/>
		<Component partNumber="511-500917" refDes="C430" circuitNumber="3" x="-238.413" y="134.909" z="0.000" rz="0.00"/>
		<Component partNumber="511-500917" refDes="C430" circuitNumber="4" x="-102.273" y="134.909" z="0.000" rz="0.00"/>
		<Component partNumber="511-500917" refDes="C455" circuitNumber="1" x="-227.486" y="15.029" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C455" circuitNumber="2" x="-91.346" y="15.029" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C455" circuitNumber="3" x="-227.486" y="119.829" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C455" circuitNumber="4" x="-91.346" y="119.829" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C458" circuitNumber="1" x="-227.486" y="12.897" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C458" circuitNumber="2" x="-91.346" y="12.897" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C458" circuitNumber="3" x="-227.486" y="117.697" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C458" circuitNumber="4" x="-91.346" y="117.697" z="0.000" rz="180.00"/>
		<Component partNumber="511-500917" refDes="C559" circuitNumber="1" x="-259.488" y="61.633" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C559" circuitNumber="2" x="-123.348" y="61.633" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C559" circuitNumber="3" x="-259.488" y="166.433" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C559" circuitNumber="4" x="-123.348" y="166.433" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C615" circuitNumber="1" x="-242.237" y="28.518" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C615" circuitNumber="2" x="-106.097" y="28.518" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C615" circuitNumber="3" x="-242.237" y="133.318" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C615" circuitNumber="4" x="-106.097" y="133.318" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C616" circuitNumber="1" x="-244.285" y="28.518" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C616" circuitNumber="2" x="-108.145" y="28.518" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C616" circuitNumber="3" x="-244.285" y="133.318" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C616" circuitNumber="4" x="-108.145" y="133.318" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C87" circuitNumber="1" x="-240.694" y="84.160" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C87" circuitNumber="2" x="-104.554" y="84.160" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C87" circuitNumber="3" x="-240.694" y="188.960" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C87" circuitNumber="4" x="-104.554" y="188.960" z="0.000" rz="270.00"/>
		<Component partNumber="511-500917" refDes="C98" circuitNumber="1" x="-245.688" y="83.719" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C98" circuitNumber="2" x="-109.548" y="83.719" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C98" circuitNumber="3" x="-245.688" y="188.519" z="0.000" rz="90.00"/>
		<Component partNumber="511-500917" refDes="C98" circuitNumber="4" x="-109.548" y="188.519" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C131" circuitNumber="1" x="-215.929" y="81.731" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C131" circuitNumber="2" x="-79.789" y="81.731" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C131" circuitNumber="3" x="-215.929" y="186.531" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C131" circuitNumber="4" x="-79.789" y="186.531" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C195" circuitNumber="1" x="-215.929" y="77.413" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C195" circuitNumber="2" x="-79.789" y="77.413" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C195" circuitNumber="3" x="-215.929" y="182.213" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C195" circuitNumber="4" x="-79.789" y="182.213" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C344" circuitNumber="1" x="-233.836" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C344" circuitNumber="2" x="-97.696" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C344" circuitNumber="3" x="-233.836" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C344" circuitNumber="4" x="-97.696" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C345" circuitNumber="1" x="-231.784" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C345" circuitNumber="2" x="-95.644" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C345" circuitNumber="3" x="-231.784" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C345" circuitNumber="4" x="-95.644" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C346" circuitNumber="1" x="-229.722" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C346" circuitNumber="2" x="-93.582" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C346" circuitNumber="3" x="-229.722" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C346" circuitNumber="4" x="-93.582" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C347" circuitNumber="1" x="-227.660" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C347" circuitNumber="2" x="-91.520" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C347" circuitNumber="3" x="-227.660" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C347" circuitNumber="4" x="-91.520" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C348" circuitNumber="1" x="-225.608" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C348" circuitNumber="2" x="-89.468" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C348" circuitNumber="3" x="-225.608" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C348" circuitNumber="4" x="-89.468" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C349" circuitNumber="1" x="-223.041" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C349" circuitNumber="2" x="-86.901" y="57.855" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C349" circuitNumber="3" x="-223.041" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C349" circuitNumber="4" x="-86.901" y="162.655" z="0.000" rz="270.00"/>
		<Component partNumber="511-500918" refDes="C42" circuitNumber="1" x="-222.306" y="92.780" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C42" circuitNumber="2" x="-86.166" y="92.780" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C42" circuitNumber="3" x="-222.306" y="197.580" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C42" circuitNumber="4" x="-86.166" y="197.580" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C45" circuitNumber="1" x="-226.847" y="90.431" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C45" circuitNumber="2" x="-90.707" y="90.431" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C45" circuitNumber="3" x="-226.847" y="195.231" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C45" circuitNumber="4" x="-90.707" y="195.231" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C47" circuitNumber="1" x="-223.232" y="90.327" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C47" circuitNumber="2" x="-87.092" y="90.327" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C47" circuitNumber="3" x="-223.232" y="195.127" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C47" circuitNumber="4" x="-87.092" y="195.127" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C48" circuitNumber="1" x="-221.173" y="90.327" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C48" circuitNumber="2" x="-85.033" y="90.327" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C48" circuitNumber="3" x="-221.173" y="195.127" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C48" circuitNumber="4" x="-85.033" y="195.127" z="0.000" rz="90.00"/>
		<Component partNumber="511-500918" refDes="C68" circuitNumber="1" x="-230.788" y="87.944" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C68" circuitNumber="2" x="-94.648" y="87.944" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C68" circuitNumber="3" x="-230.788" y="192.744" z="0.000" rz="180.00"/>
		<Component partNumber="511-500918" refDes="C68" circuitNumber="4" x="-94.648" y="192.744" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C134" circuitNumber="1" x="-153.040" y="82.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C134" circuitNumber="2" x="-16.900" y="82.177" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C134" circuitNumber="3" x="-153.040" y="186.977" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C134" circuitNumber="4" x="-16.900" y="186.977" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C2" circuitNumber="1" x="-213.908" y="104.089" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C2" circuitNumber="2" x="-77.768" y="104.089" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C2" circuitNumber="3" x="-213.908" y="208.889" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C2" circuitNumber="4" x="-77.768" y="208.889" z="0.000" rz="180.00"/>
		<Component partNumber="511-500919" refDes="C256" circuitNumber="1" x="-179.710" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C256" circuitNumber="2" x="-43.570" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C256" circuitNumber="3" x="-179.710" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C256" circuitNumber="4" x="-43.570" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C261" circuitNumber="1" x="-160.660" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C261" circuitNumber="2" x="-24.520" y="73.349" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C261" circuitNumber="3" x="-160.660" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C261" circuitNumber="4" x="-24.520" y="178.149" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C31" circuitNumber="1" x="-215.548" y="97.860" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C31" circuitNumber="2" x="-79.408" y="97.860" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C31" circuitNumber="3" x="-215.548" y="202.660" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C31" circuitNumber="4" x="-79.408" y="202.660" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C333" circuitNumber="1" x="-153.040" y="62.427" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C333" circuitNumber="2" x="-16.900" y="62.427" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C333" circuitNumber="3" x="-153.040" y="167.227" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C333" circuitNumber="4" x="-16.900" y="167.227" z="0.000" rz="90.00"/>
		<Component partNumber="511-500919" refDes="C353" circuitNumber="1" x="-179.710" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C353" circuitNumber="2" x="-43.570" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C353" circuitNumber="3" x="-179.710" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C353" circuitNumber="4" x="-43.570" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C360" circuitNumber="1" x="-160.660" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C360" circuitNumber="2" x="-24.520" y="57.855" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C360" circuitNumber="3" x="-160.660" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C360" circuitNumber="4" x="-24.520" y="162.655" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C388" circuitNumber="1" x="-179.710" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C388" circuitNumber="2" x="-43.570" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C388" circuitNumber="3" x="-179.710" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C388" circuitNumber="4" x="-43.570" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C393" circuitNumber="1" x="-160.660" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C393" circuitNumber="2" x="-24.520" y="42.361" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C393" circuitNumber="3" x="-160.660" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C393" circuitNumber="4" x="-24.520" y="147.161" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C397" circuitNumber="1" x="-226.089" y="42.234" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C397" circuitNumber="2" x="-89.949" y="42.234" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C397" circuitNumber="3" x="-226.089" y="147.034" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C397" circuitNumber="4" x="-89.949" y="147.034" z="0.000" rz="270.00"/>
		<Component partNumber="511-500919" refDes="C440" circuitNumber="1" x="-179.710" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C440" circuitNumber="2" x="-43.570" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C440" circuitNumber="3" x="-179.710" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C440" circuitNumber="4" x="-43.570" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C447" circuitNumber="1" x="-160.660" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C447" circuitNumber="2" x="-24.520" y="26.867" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C447" circuitNumber="3" x="-160.660" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C447" circuitNumber="4" x="-24.520" y="131.667" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C462" circuitNumber="1" x="-179.710" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C462" circuitNumber="2" x="-43.570" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C462" circuitNumber="3" x="-179.710" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C462" circuitNumber="4" x="-43.570" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C467" circuitNumber="1" x="-160.660" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C467" circuitNumber="2" x="-24.520" y="12.034" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C467" circuitNumber="3" x="-160.660" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C467" circuitNumber="4" x="-24.520" y="116.834" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C56" circuitNumber="1" x="-179.710" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C56" circuitNumber="2" x="-43.570" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C56" circuitNumber="3" x="-179.710" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C56" circuitNumber="4" x="-43.570" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C63" circuitNumber="1" x="-160.660" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C63" circuitNumber="2" x="-24.520" y="88.843" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C63" circuitNumber="3" x="-160.660" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500919" refDes="C63" circuitNumber="4" x="-24.520" y="193.643" z="0.000" rz="0.00"/>
		<Component partNumber="511-500922" refDes="C32" circuitNumber="1" x="-152.913" y="96.463" z="0.000" rz="90.00"/>
		<Component partNumber="511-500922" refDes="C32" circuitNumber="2" x="-16.773" y="96.463" z="0.000" rz="90.00"/>
		<Component partNumber="511-500922" refDes="C32" circuitNumber="3" x="-152.913" y="201.263" z="0.000" rz="90.00"/>
		<Component partNumber="511-500922" refDes="C32" circuitNumber="4" x="-16.773" y="201.263" z="0.000" rz="90.00"/>
		<Component partNumber="511-500922" refDes="C451" circuitNumber="1" x="-242.980" y="23.819" z="0.000" rz="90.00"/>
		<Component partNumber="511-500922" refDes="C451" circuitNumber="2" x="-106.840" y="23.819" z="0.000" rz="90.00"/>
		<Component partNumber="511-500922" refDes="C451" circuitNumber="3" x="-242.980" y="128.619" z="0.000" rz="90.00"/>
		<Component partNumber="511-500922" refDes="C451" circuitNumber="4" x="-106.840" y="128.619" z="0.000" rz="90.00"/>
		<Component partNumber="512-500442" refDes="FB4" circuitNumber="1" x="-228.000" y="88.109" z="0.000" rz="180.00"/>
		<Component partNumber="512-500442" refDes="FB4" circuitNumber="2" x="-91.860" y="88.109" z="0.000" rz="180.00"/>
		<Component partNumber="512-500442" refDes="FB4" circuitNumber="3" x="-228.000" y="192.909" z="0.000" rz="180.00"/>
		<Component partNumber="512-500442" refDes="FB4" circuitNumber="4" x="-91.860" y="192.909" z="0.000" rz="180.00"/>
		<Component partNumber="512-500442" refDes="FB5" circuitNumber="1" x="-245.638" y="78.597" z="0.000" rz="90.00"/>
		<Component partNumber="512-500442" refDes="FB5" circuitNumber="2" x="-109.498" y="78.597" z="0.000" rz="90.00"/>
		<Component partNumber="512-500442" refDes="FB5" circuitNumber="3" x="-245.638" y="183.397" z="0.000" rz="90.00"/>
		<Component partNumber="512-500442" refDes="FB5" circuitNumber="4" x="-109.498" y="183.397" z="0.000" rz="90.00"/>
		<Component partNumber="512-500444" refDes="FB2" circuitNumber="1" x="-236.919" y="77.059" z="0.000" rz="0.00"/>
		<Component partNumber="512-500444" refDes="FB2" circuitNumber="2" x="-100.779" y="77.059" z="0.000" rz="0.00"/>
		<Component partNumber="512-500444" refDes="FB2" circuitNumber="3" x="-236.919" y="181.859" z="0.000" rz="0.00"/>
		<Component partNumber="512-500444" refDes="FB2" circuitNumber="4" x="-100.779" y="181.859" z="0.000" rz="0.00"/>
		<Component partNumber="512-500449" refDes="FB3" circuitNumber="1" x="-240.420" y="77.853" z="0.000" rz="270.00"/>
		<Component partNumber="512-500449" refDes="FB3" circuitNumber="2" x="-104.280" y="77.853" z="0.000" rz="270.00"/>
		<Component partNumber="512-500449" refDes="FB3" circuitNumber="3" x="-240.420" y="182.653" z="0.000" rz="270.00"/>
		<Component partNumber="512-500449" refDes="FB3" circuitNumber="4" x="-104.280" y="182.653" z="0.000" rz="270.00"/>
		<Component partNumber="513-500517" refDes="D6" circuitNumber="1" x="-249.965" y="102.940" z="0.000" rz="90.00"/>
		<Component partNumber="513-500517" refDes="D6" circuitNumber="2" x="-113.825" y="102.940" z="0.000" rz="90.00"/>
		<Component partNumber="513-500517" refDes="D6" circuitNumber="3" x="-249.965" y="207.740" z="0.000" rz="90.00"/>
		<Component partNumber="513-500517" refDes="D6" circuitNumber="4" x="-113.825" y="207.740" z="0.000" rz="90.00"/>
		<Component partNumber="513-500519" refDes="D1" circuitNumber="1" x="-230.407" y="104.268" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D1" circuitNumber="2" x="-94.267" y="104.268" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D1" circuitNumber="3" x="-230.407" y="209.068" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D1" circuitNumber="4" x="-94.267" y="209.068" z="0.000" rz="0.00"/>
		<Component partNumber="513-500519" refDes="D4" circuitNumber="1" x="-252.378" y="102.940" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D4" circuitNumber="2" x="-116.238" y="102.940" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D4" circuitNumber="3" x="-252.378" y="207.740" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D4" circuitNumber="4" x="-116.238" y="207.740" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D5" circuitNumber="1" x="-251.235" y="102.940" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D5" circuitNumber="2" x="-115.095" y="102.940" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D5" circuitNumber="3" x="-251.235" y="207.740" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D5" circuitNumber="4" x="-115.095" y="207.740" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D8" circuitNumber="1" x="-221.838" y="33.109" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D8" circuitNumber="2" x="-85.698" y="33.109" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D8" circuitNumber="3" x="-221.838" y="137.909" z="0.000" rz="270.00"/>
		<Component partNumber="513-500519" refDes="D8" circuitNumber="4" x="-85.698" y="137.909" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q10" circuitNumber="1" x="-229.149" y="33.852" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q10" circuitNumber="2" x="-93.009" y="33.852" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q10" circuitNumber="3" x="-229.149" y="138.652" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q10" circuitNumber="4" x="-93.009" y="138.652" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q11" circuitNumber="1" x="-225.212" y="33.852" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q11" circuitNumber="2" x="-89.072" y="33.852" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q11" circuitNumber="3" x="-225.212" y="138.652" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q11" circuitNumber="4" x="-89.072" y="138.652" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q12" circuitNumber="1" x="-241.337" y="33.839" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q12" circuitNumber="2" x="-105.197" y="33.839" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q12" circuitNumber="3" x="-241.337" y="138.639" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q12" circuitNumber="4" x="-105.197" y="138.639" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q14" circuitNumber="1" x="-225.188" y="31.186" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q14" circuitNumber="2" x="-89.048" y="31.186" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q14" circuitNumber="3" x="-225.188" y="135.986" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q14" circuitNumber="4" x="-89.048" y="135.986" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q15" circuitNumber="1" x="-222.799" y="14.086" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q15" circuitNumber="2" x="-86.659" y="14.086" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q15" circuitNumber="3" x="-222.799" y="118.886" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q15" circuitNumber="4" x="-86.659" y="118.886" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q25" circuitNumber="1" x="-213.338" y="30.994" z="0.000" rz="0.00"/>
		<Component partNumber="513-500520" refDes="Q25" circuitNumber="2" x="-77.198" y="30.994" z="0.000" rz="0.00"/>
		<Component partNumber="513-500520" refDes="Q25" circuitNumber="3" x="-213.338" y="135.794" z="0.000" rz="0.00"/>
		<Component partNumber="513-500520" refDes="Q25" circuitNumber="4" x="-77.198" y="135.794" z="0.000" rz="0.00"/>
		<Component partNumber="513-500520" refDes="Q4" circuitNumber="1" x="-261.891" y="84.202" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q4" circuitNumber="2" x="-125.751" y="84.202" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q4" circuitNumber="3" x="-261.891" y="189.002" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q4" circuitNumber="4" x="-125.751" y="189.002" z="0.000" rz="270.00"/>
		<Component partNumber="513-500520" refDes="Q6" circuitNumber="1" x="-266.233" y="80.264" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q6" circuitNumber="2" x="-130.093" y="80.264" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q6" circuitNumber="3" x="-266.233" y="185.064" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q6" circuitNumber="4" x="-130.093" y="185.064" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q7" circuitNumber="1" x="-266.233" y="77.724" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q7" circuitNumber="2" x="-130.093" y="77.724" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q7" circuitNumber="3" x="-266.233" y="182.524" z="0.000" rz="90.00"/>
		<Component partNumber="513-500520" refDes="Q7" circuitNumber="4" x="-130.093" y="182.524" z="0.000" rz="90.00"/>
		<Component partNumber="513-500521" refDes="D10" circuitNumber="1" x="-245.018" y="19.659" z="0.000" rz="180.00"/>
		<Component partNumber="513-500521" refDes="D10" circuitNumber="2" x="-108.878" y="19.659" z="0.000" rz="180.00"/>
		<Component partNumber="513-500521" refDes="D10" circuitNumber="3" x="-245.018" y="124.459" z="0.000" rz="180.00"/>
		<Component partNumber="513-500521" refDes="D10" circuitNumber="4" x="-108.878" y="124.459" z="0.000" rz="180.00"/>
		<Component partNumber="513-500521" refDes="D11" circuitNumber="1" x="-253.138" y="18.259" z="0.000" rz="270.00"/>
		<Component partNumber="513-500521" refDes="D11" circuitNumber="2" x="-116.998" y="18.259" z="0.000" rz="270.00"/>
		<Component partNumber="513-500521" refDes="D11" circuitNumber="3" x="-253.138" y="123.059" z="0.000" rz="270.00"/>
		<Component partNumber="513-500521" refDes="D11" circuitNumber="4" x="-116.998" y="123.059" z="0.000" rz="270.00"/>
		<Component partNumber="513-500521" refDes="D12" circuitNumber="1" x="-250.838" y="18.259" z="0.000" rz="270.00"/>
		<Component partNumber="513-500521" refDes="D12" circuitNumber="2" x="-114.698" y="18.259" z="0.000" rz="270.00"/>
		<Component partNumber="513-500521" refDes="D12" circuitNumber="3" x="-250.838" y="123.059" z="0.000" rz="270.00"/>
		<Component partNumber="513-500521" refDes="D12" circuitNumber="4" x="-114.698" y="123.059" z="0.000" rz="270.00"/>
		<Component partNumber="513-500526" refDes="Q2" circuitNumber="1" x="-214.024" y="102.270" z="0.000" rz="180.00"/>
		<Component partNumber="513-500526" refDes="Q2" circuitNumber="2" x="-77.884" y="102.270" z="0.000" rz="180.00"/>
		<Component partNumber="513-500526" refDes="Q2" circuitNumber="3" x="-214.024" y="207.070" z="0.000" rz="180.00"/>
		<Component partNumber="513-500526" refDes="Q2" circuitNumber="4" x="-77.884" y="207.070" z="0.000" rz="180.00"/>
		<Component partNumber="513-500526" refDes="Q3" circuitNumber="1" x="-216.818" y="101.444" z="0.000" rz="0.00"/>
		<Component partNumber="513-500526" refDes="Q3" circuitNumber="2" x="-80.678" y="101.444" z="0.000" rz="0.00"/>
		<Component partNumber="513-500526" refDes="Q3" circuitNumber="3" x="-216.818" y="206.244" z="0.000" rz="0.00"/>
		<Component partNumber="513-500526" refDes="Q3" circuitNumber="4" x="-80.678" y="206.244" z="0.000" rz="0.00"/>
		<Component partNumber="515-500764" refDes="F1" circuitNumber="1" x="-216.347" y="95.701" z="0.000" rz="180.00"/>
		<Component partNumber="515-500764" refDes="F1" circuitNumber="2" x="-80.207" y="95.701" z="0.000" rz="180.00"/>
		<Component partNumber="515-500764" refDes="F1" circuitNumber="3" x="-216.347" y="200.501" z="0.000" rz="180.00"/>
		<Component partNumber="515-500764" refDes="F1" circuitNumber="4" x="-80.207" y="200.501" z="0.000" rz="180.00"/>
		<Component partNumber="515-500770" refDes="VR29" circuitNumber="1" x="-250.854" y="31.820" z="0.000" rz="90.00"/>
		<Component partNumber="515-500770" refDes="VR29" circuitNumber="2" x="-114.714" y="31.820" z="0.000" rz="90.00"/>
		<Component partNumber="515-500770" refDes="VR29" circuitNumber="3" x="-250.854" y="136.620" z="0.000" rz="90.00"/>
		<Component partNumber="515-500770" refDes="VR29" circuitNumber="4" x="-114.714" y="136.620" z="0.000" rz="90.00"/>
		<Component partNumber="515-500772" refDes="VR14" circuitNumber="1" x="-262.284" y="77.979" z="0.000" rz="180.00"/>
		<Component partNumber="515-500772" refDes="VR14" circuitNumber="2" x="-126.144" y="77.979" z="0.000" rz="180.00"/>
		<Component partNumber="515-500772" refDes="VR14" circuitNumber="3" x="-262.284" y="182.779" z="0.000" rz="180.00"/>
		<Component partNumber="515-500772" refDes="VR14" circuitNumber="4" x="-126.144" y="182.779" z="0.000" rz="180.00"/>
		<Component partNumber="515-500772" refDes="VR20" circuitNumber="1" x="-258.266" y="58.593" z="0.000" rz="90.00"/>
		<Component partNumber="515-500772" refDes="VR20" circuitNumber="2" x="-122.126" y="58.593" z="0.000" rz="90.00"/>
		<Component partNumber="515-500772" refDes="VR20" circuitNumber="3" x="-258.266" y="163.393" z="0.000" rz="90.00"/>
		<Component partNumber="515-500772" refDes="VR20" circuitNumber="4" x="-122.126" y="163.393" z="0.000" rz="90.00"/>
		<Component partNumber="515-500773" refDes="U3" circuitNumber="1" x="-235.868" y="102.921" z="0.000" rz="90.00"/>
		<Component partNumber="515-500773" refDes="U3" circuitNumber="2" x="-99.728" y="102.921" z="0.000" rz="90.00"/>
		<Component partNumber="515-500773" refDes="U3" circuitNumber="3" x="-235.868" y="207.721" z="0.000" rz="90.00"/>
		<Component partNumber="515-500773" refDes="U3" circuitNumber="4" x="-99.728" y="207.721" z="0.000" rz="90.00"/>
		<Component partNumber="515-500773" refDes="U9" circuitNumber="1" x="-266.221" y="83.567" z="0.000" rz="270.00"/>
		<Component partNumber="515-500773" refDes="U9" circuitNumber="2" x="-130.081" y="83.567" z="0.000" rz="270.00"/>
		<Component partNumber="515-500773" refDes="U9" circuitNumber="3" x="-266.221" y="188.367" z="0.000" rz="270.00"/>
		<Component partNumber="515-500773" refDes="U9" circuitNumber="4" x="-130.081" y="188.367" z="0.000" rz="270.00"/>
		<Component partNumber="515-500779" refDes="S1" circuitNumber="1" x="-218.788" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="515-500779" refDes="S1" circuitNumber="2" x="-82.648" y="27.274" z="0.000" rz="180.00"/>
		<Component partNumber="515-500779" refDes="S1" circuitNumber="3" x="-218.788" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="515-500779" refDes="S1" circuitNumber="4" x="-82.648" y="132.074" z="0.000" rz="180.00"/>
		<Component partNumber="520-M5" refDes="U10" circuitNumber="1" x="-182.377" y="81.096" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U10" circuitNumber="2" x="-46.237" y="81.096" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U10" circuitNumber="3" x="-182.377" y="185.896" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U10" circuitNumber="4" x="-46.237" y="185.896" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U11" circuitNumber="1" x="-163.327" y="81.096" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U11" circuitNumber="2" x="-27.187" y="81.096" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U11" circuitNumber="3" x="-163.327" y="185.896" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U11" circuitNumber="4" x="-27.187" y="185.896" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U12" circuitNumber="1" x="-145.293" y="80.187" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U12" circuitNumber="2" x="-9.153" y="80.187" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U12" circuitNumber="3" x="-145.293" y="184.987" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U12" circuitNumber="4" x="-9.153" y="184.987" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U16" circuitNumber="1" x="-182.377" y="65.602" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U16" circuitNumber="2" x="-46.237" y="65.602" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U16" circuitNumber="3" x="-182.377" y="170.402" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U16" circuitNumber="4" x="-46.237" y="170.402" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U17" circuitNumber="1" x="-163.327" y="65.602" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U17" circuitNumber="2" x="-27.187" y="65.602" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U17" circuitNumber="3" x="-163.327" y="170.402" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U17" circuitNumber="4" x="-27.187" y="170.402" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U19" circuitNumber="1" x="-145.293" y="59.887" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U19" circuitNumber="2" x="-9.153" y="59.887" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U19" circuitNumber="3" x="-145.293" y="164.687" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U19" circuitNumber="4" x="-9.153" y="164.687" z="0.000" rz="90.00"/>
		<Component partNumber="520-M5" refDes="U22" circuitNumber="1" x="-182.377" y="50.108" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U22" circuitNumber="2" x="-46.237" y="50.108" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U22" circuitNumber="3" x="-182.377" y="154.908" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U22" circuitNumber="4" x="-46.237" y="154.908" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U23" circuitNumber="1" x="-163.327" y="50.108" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U23" circuitNumber="2" x="-27.187" y="50.108" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U23" circuitNumber="3" x="-163.327" y="154.908" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U23" circuitNumber="4" x="-27.187" y="154.908" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U25" circuitNumber="1" x="-218.342" y="44.901" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U25" circuitNumber="2" x="-82.202" y="44.901" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U25" circuitNumber="3" x="-218.342" y="149.701" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U25" circuitNumber="4" x="-82.202" y="149.701" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U26" circuitNumber="1" x="-182.377" y="34.614" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U26" circuitNumber="2" x="-46.237" y="34.614" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U26" circuitNumber="3" x="-182.377" y="139.414" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U26" circuitNumber="4" x="-46.237" y="139.414" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U27" circuitNumber="1" x="-163.327" y="34.614" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U27" circuitNumber="2" x="-27.187" y="34.614" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U27" circuitNumber="3" x="-163.327" y="139.414" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U27" circuitNumber="4" x="-27.187" y="139.414" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U31" circuitNumber="1" x="-182.377" y="19.120" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U31" circuitNumber="2" x="-46.237" y="19.120" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U31" circuitNumber="3" x="-182.377" y="123.920" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U31" circuitNumber="4" x="-46.237" y="123.920" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U32" circuitNumber="1" x="-163.327" y="19.120" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U32" circuitNumber="2" x="-27.187" y="19.120" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U32" circuitNumber="3" x="-163.327" y="123.920" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U32" circuitNumber="4" x="-27.187" y="123.920" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U5" circuitNumber="1" x="-182.377" y="96.590" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U5" circuitNumber="2" x="-46.237" y="96.590" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U5" circuitNumber="3" x="-182.377" y="201.390" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U5" circuitNumber="4" x="-46.237" y="201.390" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U52" circuitNumber="1" x="-233.836" y="44.901" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U52" circuitNumber="2" x="-97.696" y="44.901" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U52" circuitNumber="3" x="-233.836" y="149.701" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U52" circuitNumber="4" x="-97.696" y="149.701" z="0.000" rz="270.00"/>
		<Component partNumber="520-M5" refDes="U6" circuitNumber="1" x="-163.327" y="96.590" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U6" circuitNumber="2" x="-27.187" y="96.590" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U6" circuitNumber="3" x="-163.327" y="201.390" z="0.000" rz="0.00"/>
		<Component partNumber="520-M5" refDes="U6" circuitNumber="4" x="-27.187" y="201.390" z="0.000" rz="0.00"/>
		<Component partNumber="520-NEA" refDes="U13" circuitNumber="1" x="-252.415" y="77.578" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U13" circuitNumber="2" x="-116.275" y="77.578" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U13" circuitNumber="3" x="-252.415" y="182.378" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U13" circuitNumber="4" x="-116.275" y="182.378" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U15" circuitNumber="1" x="-252.415" y="66.081" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U15" circuitNumber="2" x="-116.275" y="66.081" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U15" circuitNumber="3" x="-252.415" y="170.881" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U15" circuitNumber="4" x="-116.275" y="170.881" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U21" circuitNumber="1" x="-247.791" y="54.584" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U21" circuitNumber="2" x="-111.651" y="54.584" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U21" circuitNumber="3" x="-247.791" y="159.384" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U21" circuitNumber="4" x="-111.651" y="159.384" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U8" circuitNumber="1" x="-252.415" y="89.075" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U8" circuitNumber="2" x="-116.275" y="89.075" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U8" circuitNumber="3" x="-252.415" y="193.875" z="0.000" rz="180.00"/>
		<Component partNumber="520-NEA" refDes="U8" circuitNumber="4" x="-116.275" y="193.875" z="0.000" rz="180.00"/>
		<Component partNumber="521-501375" refDes="U4" circuitNumber="1" x="-218.823" y="102.672" z="0.000" rz="90.00"/>
		<Component partNumber="521-501375" refDes="U4" circuitNumber="2" x="-82.683" y="102.672" z="0.000" rz="90.00"/>
		<Component partNumber="521-501375" refDes="U4" circuitNumber="3" x="-218.823" y="207.472" z="0.000" rz="90.00"/>
		<Component partNumber="521-501375" refDes="U4" circuitNumber="4" x="-82.683" y="207.472" z="0.000" rz="90.00"/>
		<Component partNumber="521-501379" refDes="U28" circuitNumber="1" x="-218.878" y="33.618" z="0.000" rz="180.00"/>
		<Component partNumber="521-501379" refDes="U28" circuitNumber="2" x="-82.738" y="33.618" z="0.000" rz="180.00"/>
		<Component partNumber="521-501379" refDes="U28" circuitNumber="3" x="-218.878" y="138.418" z="0.000" rz="180.00"/>
		<Component partNumber="521-501379" refDes="U28" circuitNumber="4" x="-82.738" y="138.418" z="0.000" rz="180.00"/>
		<Component partNumber="521-501380" refDes="U1" circuitNumber="1" x="-259.617" y="103.449" z="0.000" rz="270.00"/>
		<Component partNumber="521-501380" refDes="U1" circuitNumber="2" x="-123.477" y="103.449" z="0.000" rz="270.00"/>
		<Component partNumber="521-501380" refDes="U1" circuitNumber="3" x="-259.617" y="208.249" z="0.000" rz="270.00"/>
		<Component partNumber="521-501380" refDes="U1" circuitNumber="4" x="-123.477" y="208.249" z="0.000" rz="270.00"/>
		<Component partNumber="521-501380" refDes="U18" circuitNumber="1" x="-243.615" y="62.301" z="0.000" rz="270.00"/>
		<Component partNumber="521-501380" refDes="U18" circuitNumber="2" x="-107.475" y="62.301" z="0.000" rz="270.00"/>
		<Component partNumber="521-501380" refDes="U18" circuitNumber="3" x="-243.615" y="167.101" z="0.000" rz="270.00"/>
		<Component partNumber="521-501380" refDes="U18" circuitNumber="4" x="-107.475" y="167.101" z="0.000" rz="270.00"/>
		<Component partNumber="531-500464" refDes="J2" circuitNumber="1" x="-259.788" y="13.541" z="0.000" rz="180.00"/>
		<Component partNumber="531-500464" refDes="J2" circuitNumber="2" x="-123.648" y="13.541" z="0.000" rz="180.00"/>
		<Component partNumber="531-500464" refDes="J2" circuitNumber="3" x="-259.788" y="118.341" z="0.000" rz="180.00"/>
		<Component partNumber="531-500464" refDes="J2" circuitNumber="4" x="-123.648" y="118.341" z="0.000" rz="180.00"/>
		<Component partNumber="541-500133" refDes="SP1" circuitNumber="1" x="-267.038" y="99.609" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP1" circuitNumber="2" x="-130.898" y="99.609" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP1" circuitNumber="3" x="-267.038" y="204.409" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP1" circuitNumber="4" x="-130.898" y="204.409" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP2" circuitNumber="1" x="-206.838" y="99.609" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP2" circuitNumber="2" x="-70.698" y="99.609" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP2" circuitNumber="3" x="-206.838" y="204.409" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP2" circuitNumber="4" x="-70.698" y="204.409" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP3" circuitNumber="1" x="-267.038" y="12.459" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP3" circuitNumber="2" x="-130.898" y="12.459" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP3" circuitNumber="3" x="-267.038" y="117.259" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP3" circuitNumber="4" x="-130.898" y="117.259" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP4" circuitNumber="1" x="-206.838" y="12.459" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP4" circuitNumber="2" x="-70.698" y="12.459" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP4" circuitNumber="3" x="-206.838" y="117.259" z="0.000" rz="0.00"/>
		<Component partNumber="541-500133" refDes="SP4" circuitNumber="4" x="-70.698" y="117.259" z="0.000" rz="0.00"/>
		<Circuit number="1" type="3440CB-PD9-M5(C)" sizeX="133.600" sizeY="95.800" x="-269.738" y="10.159" z="0.000" rz="0.00"/>
		<Circuit number="2" type="3440CB-PD9-M5(C)" sizeX="133.600" sizeY="95.800" x="-133.598" y="10.159" z="0.000" rz="0.00"/>
		<Circuit number="3" type="3440CB-PD9-M5(C)" sizeX="133.600" sizeY="95.800" x="-269.738" y="114.959" z="0.000" rz="0.00"/>
		<Circuit number="4" type="3440CB-PD9-M5(C)" sizeX="133.600" sizeY="95.800" x="-133.598" y="114.959" z="0.000" rz="0.00"/>
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
					<Lane number="1" partNumber="510-500956" shapeId="SMR0402"/>
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
			<Pick boardNumber="2" alignWith="AM_3" refDes="C149" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="0_1_0_F0_1_1" boardNumber="2" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="0_1_0_F3_3_4" boardNumber="2" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="0_1_0_F2_6_3" boardNumber="2" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C172" circuitNumber="2" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C227" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C743" circuitNumber="2" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C315" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C172" circuitNumber="4" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C180" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C272" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C127" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C209" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C164" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C168" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C170" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C419" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C319" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C278" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C243" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C206" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C205" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C181" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C173" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C168" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C154" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_3" refDes="C743" circuitNumber="4" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<ReadFiducial refId="0_1_1_F2_3_3" boardNumber="2" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="0_1_1_F0_5_1" boardNumber="2" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="0_1_1_F0_6_1" boardNumber="2" refDes="F0_6" circuitNumber="1"/>
			<ReadBadmark boardNumber="2" alignWith="AM_5" refDes="B0" circuitNumber="1"/>
			<ReadBadmark boardNumber="2" alignWith="AM_5" refDes="B2" circuitNumber="3"/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C145" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="R288" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="R50" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="R288" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C283" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C280" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C250" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C215" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C210" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C198" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C140" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C154" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C120" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C149" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C91" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C323" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C93" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C278" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C379" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C239" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="C211" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_5" refDes="R50" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="1" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_8" refDes="C172" circuitNumber="3" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="0_1_2_F0_0_1" boardNumber="2" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="0_1_2_F2_3_3" boardNumber="2" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="0_1_2_F2_2_3" boardNumber="2" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C341" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C172" circuitNumber="1" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C235" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C743" circuitNumber="1" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C338" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C204" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C278" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C158" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C270" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C150" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C230" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C73" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C196" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C420" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_8" refDes="C743" circuitNumber="3" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="1" maxBoardNumber="1">
			<ReadFiducial refId="0_0_3_F3_3_4" boardNumber="1" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="0_0_3_F1_5_2" boardNumber="1" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="0_0_3_F1_6_2" boardNumber="1" refDes="F1_6" circuitNumber="2"/>
			<ReadBadmark boardNumber="1" alignWith="AM_1" refDes="B1" circuitNumber="2"/>
			<ReadBadmark boardNumber="1" alignWith="AM_1" refDes="B3" circuitNumber="4"/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C170" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="R50" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="R288" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C277" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="R288" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C325" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C248" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C230" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C200" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C184" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C146" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C137" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C317" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C267" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C226" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C209" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="C184" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="1" alignWith="AM_1" refDes="R50" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="2">
		<Index minBoardNumber="2" maxBoardNumber="3">
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="3" alignWith="AM_14" refDes="U9" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_2_0_F0_0_1" boardNumber="3" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="1_2_0_F2_2_3" boardNumber="3" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_14" refDes="C375" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_14" refDes="U9" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_14" refDes="C286" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_14" refDes="C33" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C258" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_1_0_F1_1_2" boardNumber="2" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="1_1_0_F1_6_2" boardNumber="2" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="1_1_0_F3_1_4" boardNumber="2" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C467" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C469" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C393" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C366" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C261" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C258" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C63" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C264" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C440" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C7" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C360" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C8" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C469" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C465" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C367" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C366" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C264" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_2" refDes="C8" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_4" refDes="U3" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_1_1_F1_5_2" boardNumber="2" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="1_1_1_F3_3_4" boardNumber="2" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="1_1_1_F3_4_4" boardNumber="2" refDes="F3_4" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C397" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C328" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C397" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C305" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="U3" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C265" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="F1" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C304" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="F1" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C284" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C99" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C111" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C52" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C460" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C384" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C316" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C308" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C301" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C191" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C88" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C52" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_4" refDes="C2" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_7" refDes="U9" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_1_2_F0_1_1" boardNumber="2" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="1_1_2_F3_0_4" boardNumber="2" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="1_1_2_F2_6_3" boardNumber="2" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C258" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C467" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C264" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C447" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C33" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="U9" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C7" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C261" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C465" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C56" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C469" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C462" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C256" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_7" refDes="C4" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_11" refDes="F1" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="1_1_3_F0_5_1" boardNumber="2" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="1_1_3_F2_3_3" boardNumber="2" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="1_1_3_F2_4_3" boardNumber="2" refDes="F2_4" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C460" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="U3" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C406" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="F1" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C331" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C377" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C316" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C309" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C302" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C312" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C99" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C111" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C406" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C384" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C316" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C308" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C301" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="C265" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_11" refDes="U3" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="3">
		<Index minBoardNumber="3" maxBoardNumber="3">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R23" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="2_2_0_F0_1_1" boardNumber="3" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="2_2_0_F0_4_1" boardNumber="3" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="2_2_0_F2_6_3" boardNumber="3" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R196" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R194" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R189" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R122" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="C329" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R138" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R59" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R139" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R58" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R116" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R193" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R63" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R122" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R54" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R197" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R189" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R138" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R128" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R115" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R62" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R53" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_13" refDes="R22" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_17" refDes="C167" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="2_2_1_F0_0_1" boardNumber="3" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="2_2_1_F2_5_3" boardNumber="3" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="2_2_1_F2_2_3" boardNumber="3" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R148" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C416" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R155" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C267" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R99" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C211" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R96" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C207" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R147" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C177" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="R96" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C38" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C37" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C414" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C315" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C285" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C272" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C230" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C226" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C215" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C170" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_17" refDes="C137" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="3">
			<Pick boardNumber="2" alignWith="AM_6" refDes="R108" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="2_1_2_F1_1_2" boardNumber="2" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="2_1_2_F1_4_2" boardNumber="2" refDes="F1_4" circuitNumber="2"/>
			<ReadFiducial refId="2_1_2_F3_6_4" boardNumber="2" refDes="F3_6" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R196" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R191" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R189" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R126" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R190" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R87" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R138" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R163" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R116" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R115" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R62" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R196" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R189" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R138" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_6" refDes="R111" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="2">
			<Pick boardNumber="2" alignWith="AM_10" refDes="R96" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="2_1_3_F1_5_2" boardNumber="2" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="2_1_3_F3_2_4" boardNumber="2" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="2_1_3_F3_4_4" boardNumber="2" refDes="F3_4" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C415" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R148" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C278" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R99" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C215" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R96" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C178" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R147" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C101" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R148" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C100" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="R99" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C407" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C285" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C251" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C220" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C201" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="2" alignWith="AM_10" refDes="C178" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="4">
		<Index minBoardNumber="3" maxBoardNumber="3">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C69" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="3_2_0_F1_1_2" boardNumber="3" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="3_2_0_F3_6_4" boardNumber="3" refDes="F3_6" circuitNumber="4"/>
			<ReadFiducial refId="3_2_0_F1_5_2" boardNumber="3" refDes="F1_5" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C354" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C449" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C223" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C310" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C142" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C175" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C83" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C69" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C17" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C65" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C439" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C468" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C449" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C394" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C362" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C310" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C266" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C257" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C175" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C112" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_12" refDes="C44" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_16" refDes="C183" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="3_2_1_F1_5_2" boardNumber="3" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="3_2_1_F3_2_4" boardNumber="3" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="3_2_1_F0_1_1" boardNumber="3" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C407" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C246" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C319" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C300" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C314" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C183" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C481" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C454" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C163" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C205" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C300" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C416" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C245" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C246" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C481" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C232" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C272" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C189" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="R142" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C124" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="R142" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C237" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_16" refDes="C163" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_19" refDes="C203" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="3_2_2_F0_5_1" boardNumber="3" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="3_2_2_F2_6_3" boardNumber="3" refDes="F2_6" circuitNumber="3"/>
			<ReadFiducial refId="3_2_2_F0_1_1" boardNumber="3" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C468" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C359" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C394" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C330" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C266" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C223" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C175" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C105" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C112" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C62" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C65" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C448" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C361" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_19" refDes="C83" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="2" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_22" refDes="C183" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="3_2_3_F0_0_1" boardNumber="3" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="3_2_3_F2_5_3" boardNumber="3" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="3_2_3_F2_2_3" boardNumber="3" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C417" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C454" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C246" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C276" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C300" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C220" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C183" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C174" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C163" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C100" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C185" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C300" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C415" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C161" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C246" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C71" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="R142" circuitNumber="1" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="R142" circuitNumber="3" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_22" refDes="C163" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="5">
		<Index minBoardNumber="4" maxBoardNumber="4">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R59" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_3_0_F0_5_1" boardNumber="4" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="4_3_0_F2_3_3" boardNumber="4" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="4_3_0_F0_6_1" boardNumber="4" refDes="F0_6" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C75" circuitNumber="1" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C298" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R191" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C275" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C153" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R136" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C75" circuitNumber="3" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C41" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R109" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C233" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C298" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R208" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C200" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C153" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R137" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C181" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="R111" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C167" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C156" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C151" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C267" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C178" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C154" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_24" refDes="C41" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_27" refDes="C77" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_3_1_F0_0_1" boardNumber="4" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="4_3_1_F2_3_3" boardNumber="4" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="4_3_1_F2_2_3" boardNumber="4" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C453" circuitNumber="1" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C414" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C12" circuitNumber="1" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C297" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C453" circuitNumber="3" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C247" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C313" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C187" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C77" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C317" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C313" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C268" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C382" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C247" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C382" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C205" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C196" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C149" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_27" refDes="C12" circuitNumber="3" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R194" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_2_1_F1_1_2" boardNumber="3" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="4_2_1_F1_6_2" boardNumber="3" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="4_2_1_F3_1_4" boardNumber="3" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_15" refDes="R122" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_18" refDes="C75" circuitNumber="4" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_2_2_F1_5_2" boardNumber="3" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="4_2_2_F3_3_4" boardNumber="3" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="4_2_2_F1_6_2" boardNumber="3" refDes="F1_6" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C153" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="R207" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C267" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C41" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="R155" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C153" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C211" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C75" circuitNumber="2" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C243" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C167" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C145" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C187" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C151" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_18" refDes="C41" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="3">
			<Pick boardNumber="3" alignWith="AM_21" refDes="C77" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="4_2_3_F0_1_1" boardNumber="3" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="4_2_3_F3_3_4" boardNumber="3" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="4_2_3_F2_6_3" boardNumber="3" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C453" circuitNumber="2" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C374" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C12" circuitNumber="2" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C386" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C453" circuitNumber="4" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C268" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R193" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C382" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C206" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="R163" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C382" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C173" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C313" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C417" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C298" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C77" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C313" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C298" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_21" refDes="C12" circuitNumber="4" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="6">
		<Index minBoardNumber="4" maxBoardNumber="4">
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C40" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_3_0_F1_5_2" boardNumber="4" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="5_3_0_F3_3_4" boardNumber="4" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="5_3_0_F3_4_4" boardNumber="4" refDes="F3_4" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C343" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C31" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C67" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C293" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C40" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C244" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C459" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C312" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C385" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C307" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C254" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C152" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C225" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C331" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C106" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C309" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C90" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C303" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C67" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C284" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C244" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_23" refDes="C3" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_26" refDes="C56" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_3_1_F0_1_1" boardNumber="4" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="5_3_1_F3_0_4" boardNumber="4" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="5_3_1_F2_6_3" boardNumber="4" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C391" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C440" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C366" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C464" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C360" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C390" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C256" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C367" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C372" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C286" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C263" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C134" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C49" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C440" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C106" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C63" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C84" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C5" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C464" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C396" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C263" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C49" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_26" refDes="C6" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_30" refDes="C253" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_3_2_F0_5_1" boardNumber="4" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="5_3_2_F2_0_3" boardNumber="4" refDes="F2_0" circuitNumber="3"/>
			<ReadFiducial refId="5_3_2_F2_4_3" boardNumber="4" refDes="F2_4" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C254" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C343" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C90" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C308" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C40" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C293" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C459" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C244" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C284" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C343" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C312" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C304" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C302" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_30" refDes="C191" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="3" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_33" refDes="C192" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_3_3_F0_0_1" boardNumber="4" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="5_3_3_F2_2_3" boardNumber="4" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_33" refDes="C375" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_33" refDes="C369" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_33" refDes="C342" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_33" refDes="C369" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C263" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="5_2_3_F1_1_2" boardNumber="3" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="5_2_3_F1_6_2" boardNumber="3" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="5_2_3_F3_1_4" boardNumber="3" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C465" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C462" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C470" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C360" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C395" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C256" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C372" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C56" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C263" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C393" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C5" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C6" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C464" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="3" alignWith="AM_20" refDes="C6" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="7">
		<Index minBoardNumber="4" maxBoardNumber="5">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C85" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="6_4_0_F0_0_1" boardNumber="5" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="6_4_0_F2_5_3" boardNumber="5" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="6_4_0_F2_2_3" boardNumber="5" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C435" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C374" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C85" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C238" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C386" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C217" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C268" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C164" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C206" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C114" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C170" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C94" circuitNumber="1" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C419" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C94" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C386" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C16" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C282" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C248" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C206" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C220" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C198" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C177" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C156" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_36" refDes="C16" circuitNumber="3" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="5">
			<Pick boardNumber="4" alignWith="AM_25" refDes="C57" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="6_3_1_F1_1_2" boardNumber="4" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="6_3_1_F1_4_2" boardNumber="4" refDes="F1_4" circuitNumber="2"/>
			<ReadFiducial refId="6_3_1_F3_6_4" boardNumber="4" refDes="F3_6" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C468" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C441" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C442" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C439" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C362" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C352" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C257" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C330" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C262" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C364" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C58" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C105" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C463" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C57" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C442" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C441" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C389" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C55" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C355" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C329" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_25" refDes="C62" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_29" refDes="C85" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="6_3_2_F1_5_2" boardNumber="4" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="6_3_2_F3_2_4" boardNumber="4" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="6_3_2_F3_4_4" boardNumber="4" refDes="F3_4" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C321" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C251" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C222" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C201" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C190" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C181" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C143" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C85" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C116" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C94" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C219" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C94" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C16" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_29" refDes="C16" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_32" refDes="C359" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="6_3_3_F0_1_1" boardNumber="4" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="6_3_3_F0_4_1" boardNumber="4" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="6_3_3_F2_6_3" boardNumber="4" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C463" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C448" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C389" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C352" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C355" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C738" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C262" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C203" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C109" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C97" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C58" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C57" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C442" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C446" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C449" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C389" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C394" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C362" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_32" refDes="C355" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="8">
		<Index minBoardNumber="5" maxBoardNumber="5">
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C457" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="7_4_0_F0_1_1" boardNumber="5" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="7_4_0_F3_2_4" boardNumber="5" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="7_4_0_F1_5_2" boardNumber="5" refDes="F1_5" circuitNumber="2"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C159" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="R359" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C228" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="R310" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C193" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="R103" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C129" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="R92" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C91" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="R70" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C335" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="R39" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C273" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="R310" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C231" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="R103" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C208" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C176" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_35" refDes="C95" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_38" refDes="C17" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="7_4_1_F0_1_1" boardNumber="5" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="7_4_1_F2_6_3" boardNumber="5" refDes="F2_6" circuitNumber="3"/>
			<ReadFiducial refId="7_4_1_F0_5_1" boardNumber="5" refDes="F0_5" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="R364" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C446" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="R359" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C354" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="R346" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C364" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="R364" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C322" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="R359" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C142" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C83" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C44" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C352" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C364" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C322" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C223" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C105" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C62" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C64" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="C57" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_38" refDes="R346" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_41" refDes="C132" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="7_4_2_F0_0_1" boardNumber="5" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="7_4_2_F2_5_3" boardNumber="5" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="7_4_2_F2_2_3" boardNumber="5" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C271" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C229" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C166" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C165" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C157" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C124" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C454" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C273" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C228" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C194" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C169" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C159" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_41" refDes="C118" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="4" maxBoardNumber="4">
			<Pick boardNumber="4" alignWith="AM_31" refDes="C105" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="7_3_3_F1_1_2" boardNumber="4" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="7_3_3_F3_6_4" boardNumber="4" refDes="F3_6" circuitNumber="4"/>
			<ReadFiducial refId="7_3_3_F1_5_2" boardNumber="4" refDes="F1_5" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="R364" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C448" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="R346" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C359" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="R364" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C322" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C203" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C299" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C64" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C448" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C354" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C361" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C330" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C299" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C203" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C142" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="4" alignWith="AM_31" refDes="C17" circuitNumber="4" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="9">
		<Index minBoardNumber="5" maxBoardNumber="6">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C71" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="8_5_0_F0_0_1" boardNumber="6" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="8_5_0_F2_3_3" boardNumber="6" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="8_5_0_F2_2_3" boardNumber="6" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="D6" circuitNumber="1" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C336" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C241" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C285" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C180" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C481" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C127" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C243" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C96" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C36" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C78" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C407" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="R68" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C297" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C270" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C243" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C207" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C187" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C181" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C168" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C173" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="C148" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_46" refDes="D6" circuitNumber="3" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_37" refDes="C80" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="8_4_1_F1_5_2" boardNumber="5" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="8_4_1_F3_3_4" boardNumber="5" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="8_4_1_F1_6_2" boardNumber="5" refDes="F1_6" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="R68" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="R210" circuitNumber="2" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C281" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="R210" circuitNumber="4" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C237" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="R51" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C208" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="R68" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C136" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C323" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C92" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C212" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C379" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C323" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C169" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C282" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C239" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C211" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C198" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C167" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="C148" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_37" refDes="R51" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_40" refDes="C15" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="8_4_2_F0_1_1" boardNumber="5" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="8_4_2_F3_3_4" boardNumber="5" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="8_4_2_F2_6_3" boardNumber="5" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="D6" circuitNumber="2" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C196" circuitNumber="2" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C423" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C420" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C326" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C325" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C235" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C280" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C274" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C247" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C230" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="C196" circuitNumber="4" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_40" refDes="D6" circuitNumber="4" feederNumber="5" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_43" refDes="R68" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="8_4_3_F0_5_1" boardNumber="5" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="8_4_3_F2_3_3" boardNumber="5" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="8_4_3_F2_6_3" boardNumber="5" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="R210" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C405" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="R210" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C320" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="R51" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C190" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C319" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C138" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C239" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C119" circuitNumber="1" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C201" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C435" circuitNumber="3" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C173" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C145" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C137" circuitNumber="1" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C275" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="C251" circuitNumber="3" feederNumber="4" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_43" refDes="R51" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="10">
		<Index minBoardNumber="6" maxBoardNumber="6">
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C7" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_5_0_F0_1_1" boardNumber="6" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="9_5_0_F3_0_4" boardNumber="6" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="9_5_0_F2_6_3" boardNumber="6" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C342" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C390" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C393" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C259" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C388" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C192" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C367" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C429" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C286" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C259" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C353" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C192" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C4" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C333" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C8" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C63" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C391" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C447" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C375" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C258" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C264" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C33" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C8" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_44" refDes="C5" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_47" refDes="C52" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_5_1_F0_5_1" boardNumber="6" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="9_5_1_F2_0_3" boardNumber="6" refDes="F2_0" circuitNumber="3"/>
			<ReadFiducial refId="9_5_1_F2_4_3" boardNumber="6" refDes="F2_4" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C385" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C384" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C397" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C89" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C305" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C67" circuitNumber="1" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C301" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C385" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C265" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C397" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C254" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C307" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C89" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C191" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C67" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C152" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C3" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C460" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C331" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C309" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C303" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C152" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_47" refDes="C40" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_50" refDes="C33" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_5_2_F0_0_1" boardNumber="6" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="9_5_2_F2_2_3" boardNumber="6" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_50" refDes="C286" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_50" refDes="C429" circuitNumber="3" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C259" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_4_2_F1_1_2" boardNumber="5" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="9_4_2_F1_6_2" boardNumber="5" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="9_4_2_F3_1_4" boardNumber="5" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C447" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C259" circuitNumber="2" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C388" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C396" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C391" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C134" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C4" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C462" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C388" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_39" refDes="C5" circuitNumber="4" feederNumber="10" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="5">
			<Pick boardNumber="5" alignWith="AM_42" refDes="C265" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="9_4_3_F1_5_2" boardNumber="5" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="9_4_3_F3_3_4" boardNumber="5" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="9_4_3_F3_4_4" boardNumber="5" refDes="F3_4" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C460" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C406" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C384" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C377" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C331" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C316" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C309" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C301" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C253" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C303" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C191" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C88" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C3" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C406" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C343" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C312" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C304" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C302" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="5" alignWith="AM_42" refDes="C56" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="11">
		<Index minBoardNumber="6" maxBoardNumber="7">
			<ToolbitExchange toolbitType="CPL4"/>
			<Pick boardNumber="6" alignWith="AM_45" refDes="C32" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="10_5_0_F1_1_2" boardNumber="6" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="10_5_0_F1_4_2" boardNumber="6" refDes="F1_4" circuitNumber="2"/>
			<ReadFiducial refId="10_5_0_F3_6_4" boardNumber="6" refDes="F3_6" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_45" refDes="C32" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q6" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="10_5_1_F3_5_4" boardNumber="6" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="10_5_1_F1_0_2" boardNumber="6" refDes="F1_0" circuitNumber="2"/>
			<ReadFiducial refId="10_5_1_F3_2_4" boardNumber="6" refDes="F3_2" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="C348" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q15" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="C48" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q10" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="C349" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q11" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q14" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q12" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q25" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q4" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q15" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q25" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q14" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q12" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q10" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q11" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q7" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_48" refDes="Q4" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_51" refDes="Q6" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="10_5_2_F0_1_1" boardNumber="6" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="10_5_2_F0_4_1" boardNumber="6" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="10_5_2_F2_6_3" boardNumber="6" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_51" refDes="Q7" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_51" refDes="C32" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="5" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q6" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="10_5_3_F2_5_3" boardNumber="6" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="10_5_3_F0_0_1" boardNumber="6" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="10_5_3_F2_2_3" boardNumber="6" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C349" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q15" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="C347" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q10" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q11" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q14" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q12" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q25" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q7" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q6" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q4" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q15" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q14" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q25" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q10" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q11" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q12" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q7" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_53" refDes="Q4" circuitNumber="3" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="12">
		<Index minBoardNumber="7" maxBoardNumber="7">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C30" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="11_6_0_F0_5_1" boardNumber="7" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="11_6_0_F2_6_3" boardNumber="7" refDes="F2_6" circuitNumber="3"/>
			<ReadFiducial refId="11_6_0_F0_1_1" boardNumber="7" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="R112" circuitNumber="1" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C29" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="R112" circuitNumber="3" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C30" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C449" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C310" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C314" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C76" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C69" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C468" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C463" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C314" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C310" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C266" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C262" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C175" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C112" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C76" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C65" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_56" refDes="C29" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="7">
			<Pick boardNumber="7" alignWith="AM_58" refDes="C20" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="11_6_1_F0_0_1" boardNumber="7" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="11_6_1_F2_5_3" boardNumber="7" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="11_6_1_F2_2_3" boardNumber="7" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C407" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C269" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C404" circuitNumber="1" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C249" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C184" circuitNumber="3" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C23" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="R69" circuitNumber="3" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C20" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C741" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C24" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C28" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C19" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C742" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C27" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C269" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C249" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C742" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C741" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C23" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C24" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C27" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_58" refDes="C28" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_52" refDes="R112" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="11_5_2_F1_5_2" boardNumber="6" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="11_5_2_F3_6_4" boardNumber="6" refDes="F3_6" circuitNumber="4"/>
			<ReadFiducial refId="11_5_2_F1_1_2" boardNumber="6" refDes="F1_1" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C30" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="R112" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C463" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C389" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C266" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C112" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_52" refDes="C30" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="6">
			<Pick boardNumber="6" alignWith="AM_54" refDes="C27" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="11_5_3_F0_1_1" boardNumber="6" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="11_5_3_F3_2_4" boardNumber="6" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="11_5_3_F1_5_2" boardNumber="6" refDes="F1_5" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C419" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C269" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C297" circuitNumber="2" feederNumber="25" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C19" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C742" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C23" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C27" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C29" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C249" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C29" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C19" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C20" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C742" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C741" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C23" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C24" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="6" alignWith="AM_54" refDes="C28" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="13">
		<Index minBoardNumber="7" maxBoardNumber="7">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C93" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="12_6_0_F1_5_2" boardNumber="7" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="12_6_0_F3_3_4" boardNumber="7" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="12_6_0_F3_6_4" boardNumber="7" refDes="F3_6" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C216" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C165" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C161" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C118" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C435" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C281" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C245" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C234" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C232" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C210" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C197" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C188" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C160" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C136" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C130" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C125" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C118" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C114" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C116" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_57" refDes="C91" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="7">
			<Pick boardNumber="7" alignWith="AM_59" refDes="C72" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="12_6_1_F0_1_1" boardNumber="7" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="12_6_1_F3_3_4" boardNumber="7" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="12_6_1_F2_6_3" boardNumber="7" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C241" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C202" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C179" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C157" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C138" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C128" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C119" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C110" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C79" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C72" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C306" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C274" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C224" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C204" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C180" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C179" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C157" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C147" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C103" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_59" refDes="C71" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="7">
			<Pick boardNumber="7" alignWith="AM_61" refDes="C110" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="12_6_2_F0_5_1" boardNumber="7" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="12_6_2_F2_3_3" boardNumber="7" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="12_6_2_F2_6_3" boardNumber="7" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C296" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C133" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C405" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C252" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C232" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C221" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C185" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C155" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C150" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C124" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C115" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_61" refDes="C96" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="6" maxBoardNumber="7">
			<Pick boardNumber="7" alignWith="AM_63" refDes="C216" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="12_6_3_F0_0_1" boardNumber="7" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="12_6_3_F2_3_3" boardNumber="7" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="12_6_3_F2_2_3" boardNumber="7" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="C335" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="C197" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="C294" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="C241" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="C180" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U13" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U21" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U15" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U13" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U8" circuitNumber="1" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U21" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U15" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_63" refDes="U8" circuitNumber="3" feederNumber="2" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="14">
		<Index minBoardNumber="8" maxBoardNumber="8">
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C111" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="13_7_0_F0_5_1" boardNumber="8" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="13_7_0_F2_0_3" boardNumber="8" refDes="F2_0" circuitNumber="3"/>
			<ReadFiducial refId="13_7_0_F2_4_3" boardNumber="8" refDes="F2_4" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C328" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C253" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C304" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C303" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C88" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C52" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C377" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C328" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C305" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C307" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C284" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C293" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C244" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C99" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C88" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="C3" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="U52" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_66" refDes="U52" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="8">
			<Pick boardNumber="7" alignWith="AM_60" refDes="U17" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="13_6_1_F1_1_2" boardNumber="7" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="13_6_1_F1_6_2" boardNumber="7" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="13_6_1_F3_1_4" boardNumber="7" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U26" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U17" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U10" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U5" circuitNumber="2" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U31" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U26" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U22" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="U11" circuitNumber="4" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL3"/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="C4" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="C390" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="C367" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="C391" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="C390" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_60" refDes="C7" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="7">
			<Pick boardNumber="7" alignWith="AM_62" refDes="C99" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="13_6_2_F1_5_2" boardNumber="7" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="13_6_2_F3_0_4" boardNumber="7" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="13_6_2_F3_4_4" boardNumber="7" refDes="F3_4" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C375" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C308" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C302" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C377" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C328" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C305" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C307" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C293" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C253" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C152" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_62" refDes="C111" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="7">
			<Pick boardNumber="7" alignWith="AM_64" refDes="C465" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="13_6_3_F0_1_1" boardNumber="7" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="13_6_3_F3_0_4" boardNumber="7" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="13_6_3_F2_6_3" boardNumber="7" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="C469" circuitNumber="1" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="C366" circuitNumber="3" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U16" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U32" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U27" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U17" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U6" circuitNumber="1" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U32" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U26" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U23" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U19" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_64" refDes="U12" circuitNumber="3" feederNumber="11" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="15">
		<Index minBoardNumber="8" maxBoardNumber="8">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R348" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="14_7_0_F1_0_2" boardNumber="8" refDes="F1_0" circuitNumber="2"/>
			<ReadFiducial refId="14_7_0_F3_5_4" boardNumber="8" refDes="F3_5" circuitNumber="4"/>
			<ReadFiducial refId="14_7_0_F3_2_4" boardNumber="8" refDes="F3_2" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R323" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R169" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R318" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R132" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R320" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R121" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R335" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R105" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R327" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R84" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R328" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R78" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R332" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R71" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R343" circuitNumber="2" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R324" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R319" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R334" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R337" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R330" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R333" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R344" circuitNumber="4" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_67" refDes="R13" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U5" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="14_7_1_F0_1_1" boardNumber="8" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="14_7_1_F0_4_1" boardNumber="8" refDes="F0_4" circuitNumber="1"/>
			<ReadFiducial refId="14_7_1_F2_6_3" boardNumber="8" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U22" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U23" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U16" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U10" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U19" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U12" circuitNumber="1" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U27" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U11" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U10" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="U6" circuitNumber="3" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="R426" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="R426" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="R97" circuitNumber="2" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_70" refDes="R97" circuitNumber="4" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="8">
			<Pick boardNumber="8" alignWith="AM_72" refDes="R344" circuitNumber="3" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="14_7_2_F0_0_1" boardNumber="8" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="14_7_2_F2_5_3" boardNumber="8" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="14_7_2_F2_2_3" boardNumber="8" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R325" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R322" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R321" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R336" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R329" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R331" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R333" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R344" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R337" circuitNumber="3" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R324" circuitNumber="3" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R319" circuitNumber="3" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R334" circuitNumber="3" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R333" circuitNumber="3" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_72" refDes="R13" circuitNumber="3" feederNumber="16" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="7" maxBoardNumber="8">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U6" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="14_6_3_F1_1_2" boardNumber="7" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="14_6_3_F1_4_2" boardNumber="7" refDes="F1_4" circuitNumber="2"/>
			<ReadFiducial refId="14_6_3_F3_6_4" boardNumber="7" refDes="F3_6" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U31" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U23" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U19" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U12" circuitNumber="2" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U27" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U19" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U16" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U12" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U10" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="7" alignWith="AM_65" refDes="U5" circuitNumber="4" feederNumber="18" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="16">
		<Index minBoardNumber="8" maxBoardNumber="9">
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R348" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="15_8_0_F0_0_1" boardNumber="9" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="15_8_0_F2_5_3" boardNumber="9" refDes="F2_5" circuitNumber="3"/>
			<ReadFiducial refId="15_8_0_F2_2_3" boardNumber="9" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R254" circuitNumber="1" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R186" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R168" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R121" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R105" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R97" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R78" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R71" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R43" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R13" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R172" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R168" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R169" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R171" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R132" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R121" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R426" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R105" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R97" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R84" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R78" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R71" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_77" refDes="R43" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<Pick boardNumber="8" alignWith="AM_71" refDes="R57" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="15_7_1_F1_5_2" boardNumber="8" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="15_7_1_F3_6_4" boardNumber="8" refDes="F3_6" circuitNumber="4"/>
			<ReadFiducial refId="15_7_1_F1_1_2" boardNumber="8" refDes="F1_1" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R350" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R163" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R350" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R136" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R151" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R110" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R94" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R58" circuitNumber="2" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R208" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R134" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R110" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="R22" circuitNumber="4" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="U16" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="U25" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="U32" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="U22" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_71" refDes="U25" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<Pick boardNumber="8" alignWith="AM_73" refDes="U52" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="15_7_2_F1_5_2" boardNumber="8" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="15_7_2_F3_2_4" boardNumber="8" refDes="F3_2" circuitNumber="4"/>
			<ReadFiducial refId="15_7_2_F0_1_1" boardNumber="8" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="U52" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="R168" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="R186" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="R173" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="R168" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="R121" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="R84" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="R78" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="R348" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_73" refDes="R43" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<Pick boardNumber="8" alignWith="AM_75" refDes="R126" circuitNumber="1" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="15_7_3_F0_5_1" boardNumber="8" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="15_7_3_F2_4_3" boardNumber="8" refDes="F2_4" circuitNumber="3"/>
			<ReadFiducial refId="15_7_3_F0_1_1" boardNumber="8" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="R350" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="R162" circuitNumber="1" feederNumber="23" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="R350" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U22" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U25" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U31" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U26" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U11" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U5" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U31" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U25" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_75" refDes="U17" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="17">
		<Index minBoardNumber="9" maxBoardNumber="9">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="U13" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="16_8_0_F0_1_1" boardNumber="9" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="16_8_0_F3_3_4" boardNumber="9" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="16_8_0_F2_6_3" boardNumber="9" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="U21" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="U15" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="U13" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="U8" circuitNumber="2" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="U21" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="U15" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="U8" circuitNumber="4" feederNumber="3" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C216" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C320" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C204" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C147" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C117" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C95" circuitNumber="2" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C423" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C277" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C241" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_78" refDes="C193" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="9">
			<Pick boardNumber="9" alignWith="AM_80" refDes="C44" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="16_8_1_F0_5_1" boardNumber="9" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="16_8_1_F2_3_3" boardNumber="9" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="16_8_1_F2_6_3" boardNumber="9" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C294" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C321" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C274" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C159" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C147" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C110" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C15" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C326" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C240" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C218" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C188" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C182" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C160" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C139" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C129" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C121" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C120" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C93" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C78" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C72" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_80" refDes="C15" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="9">
			<Pick boardNumber="9" alignWith="AM_82" refDes="C133" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="16_8_2_F0_0_1" boardNumber="9" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="16_8_2_F2_3_3" boardNumber="9" refDes="F2_3" circuitNumber="3"/>
			<ReadFiducial refId="16_8_2_F2_2_3" boardNumber="9" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C277" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C227" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C179" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C121" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C117" circuitNumber="1" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C341" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C320" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C274" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C224" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C202" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C164" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_82" refDes="C127" circuitNumber="3" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="8" maxBoardNumber="8">
			<Pick boardNumber="8" alignWith="AM_76" refDes="C73" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="16_7_3_F1_5_2" boardNumber="8" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="16_7_3_F3_3_4" boardNumber="8" refDes="F3_3" circuitNumber="4"/>
			<ReadFiducial refId="16_7_3_F3_6_4" boardNumber="8" refDes="F3_6" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C279" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C218" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C189" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C161" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C150" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C132" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C124" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C110" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C119" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C115" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C96" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C92" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C79" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C81" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="8" alignWith="AM_76" refDes="C74" circuitNumber="4" feederNumber="1" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="18">
		<Index minBoardNumber="9" maxBoardNumber="10">
			<ToolbitExchange toolbitType="V7"/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U32" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="17_8_0_F1_1_2" boardNumber="9" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="17_8_0_F1_6_2" boardNumber="9" refDes="F1_6" circuitNumber="2"/>
			<ReadFiducial refId="17_8_0_F3_1_4" boardNumber="9" refDes="F3_1" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U27" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U11" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U6" circuitNumber="2" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_79" refDes="U23" circuitNumber="4" feederNumber="9" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="R60" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="17_9_0_F0_0_1" boardNumber="10" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="17_9_0_F2_2_3" boardNumber="10" refDes="F2_2" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="C368" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="R60" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="C103" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="R44" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="C102" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="C482" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="C35" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="C425" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="C339" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="C288" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="C290" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_87" refDes="R44" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="9">
			<Pick boardNumber="9" alignWith="AM_81" refDes="R366" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="17_8_1_F1_5_2" boardNumber="9" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="17_8_1_F3_0_4" boardNumber="9" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="17_8_1_F3_4_4" boardNumber="9" refDes="F3_4" circuitNumber="4"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C405" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R177" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C279" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R367" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C236" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R34" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C224" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R44" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C169" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R368" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C144" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R177" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C139" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R367" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C96" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R34" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C15" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C321" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C252" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C228" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="C190" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_81" refDes="R368" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="9">
			<Pick boardNumber="9" alignWith="AM_83" refDes="C55" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="17_8_2_F0_1_1" boardNumber="9" refDes="F0_1" circuitNumber="1"/>
			<ReadFiducial refId="17_8_2_F3_0_4" boardNumber="9" refDes="F3_0" circuitNumber="4"/>
			<ReadFiducial refId="17_8_2_F2_6_3" boardNumber="9" refDes="F2_6" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="R149" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="C425" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="R60" circuitNumber="2" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="C103" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="R149" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="C482" circuitNumber="2" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="C368" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="C339" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="C214" circuitNumber="4" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_83" refDes="R44" circuitNumber="4" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="9">
			<Pick boardNumber="9" alignWith="AM_85" refDes="R366" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="17_8_3_F0_5_1" boardNumber="9" refDes="F0_5" circuitNumber="1"/>
			<ReadFiducial refId="17_8_3_F2_0_3" boardNumber="9" refDes="F2_0" circuitNumber="3"/>
			<ReadFiducial refId="17_8_3_F2_4_3" boardNumber="9" refDes="F2_4" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C252" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R149" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C130" circuitNumber="1" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R66" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C423" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R25" circuitNumber="1" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C271" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R349" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C222" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R170" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C199" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R149" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="C186" circuitNumber="3" feederNumber="12" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R367" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R34" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R25" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_85" refDes="R368" circuitNumber="3" feederNumber="13" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
	</Actions>
	<Actions robotNumber="19">
		<Index minBoardNumber="10" maxBoardNumber="10">
			<ToolbitExchange toolbitType="V3"/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="J2" circuitNumber="2" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="18_9_0_F3_6_4" boardNumber="10" refDes="F3_6" circuitNumber="4"/>
			<ReadFiducial refId="18_9_0_F1_1_2" boardNumber="10" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="18_9_0_F0_4_1" boardNumber="10" refDes="F0_4" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="J2" circuitNumber="4" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="SL619"/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="SP1" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="SP4" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="SP2" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="SP1" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="SP4" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="SP3" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_88" refDes="SP2" circuitNumber="4" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="10">
			<Pick boardNumber="10" alignWith="AM_89" refDes="SP1" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="18_9_1_F1_5_2" boardNumber="10" refDes="F1_5" circuitNumber="2"/>
			<ReadFiducial refId="18_9_1_F2_6_3" boardNumber="10" refDes="F2_6" circuitNumber="3"/>
			<ReadFiducial refId="18_9_1_F0_0_1" boardNumber="10" refDes="F0_0" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="SP3" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="SP4" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="SP3" circuitNumber="2" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="SP2" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="SP1" circuitNumber="1" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="SP3" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="SP4" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_89" refDes="SP2" circuitNumber="3" feederNumber="26" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="10">
			<ToolbitExchange toolbitType="V3"/>
			<Pick boardNumber="10" alignWith="AM_90" refDes="J2" circuitNumber="1" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="18_9_2_F0_2_1" boardNumber="10" refDes="F0_2" circuitNumber="1"/>
			<ReadFiducial refId="18_9_2_F2_6_3" boardNumber="10" refDes="F2_6" circuitNumber="3"/>
			<ReadFiducial refId="18_9_2_F0_1_1" boardNumber="10" refDes="F0_1" circuitNumber="1"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_90" refDes="J2" circuitNumber="3" feederNumber="24" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<ToolbitExchange toolbitType="CPL2"/>
			<Pick boardNumber="10" alignWith="AM_90" refDes="U4" circuitNumber="3" feederNumber="21" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="CV_LFOV"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_90" refDes="U4" circuitNumber="1" feederNumber="21" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="CV_LFOV"/>
			<Place/>
		</Index>
		<Index minBoardNumber="9" maxBoardNumber="10">
			<Pick boardNumber="10" alignWith="AM_91" refDes="R19" circuitNumber="1" feederNumber="15" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<ReadFiducial refId="18_9_3_F2_2_3" boardNumber="10" refDes="F2_2" circuitNumber="3"/>
			<ReadFiducial refId="18_9_3_F0_0_1" boardNumber="10" refDes="F0_0" circuitNumber="1"/>
			<ReadFiducial refId="18_9_3_F2_5_3" boardNumber="10" refDes="F2_5" circuitNumber="3"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_91" refDes="R204" circuitNumber="1" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_91" refDes="C222" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_91" refDes="C128" circuitNumber="1" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B0" circuitNumber="1"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="10" alignWith="AM_91" refDes="R204" circuitNumber="3" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B2" circuitNumber="3"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_86" refDes="U4" circuitNumber="4" feederNumber="21" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="CV_LFOV"/>
			<ReadFiducial refId="18_8_3_F1_1_2" boardNumber="9" refDes="F1_1" circuitNumber="2"/>
			<ReadFiducial refId="18_8_3_F3_6_4" boardNumber="9" refDes="F3_6" circuitNumber="4"/>
			<ReadFiducial refId="18_8_3_F1_2_2" boardNumber="9" refDes="F1_2" circuitNumber="2"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_86" refDes="C115" circuitNumber="2" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_86" refDes="R204" circuitNumber="2" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_86" refDes="C202" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_86" refDes="R204" circuitNumber="4" feederNumber="20" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_86" refDes="C133" circuitNumber="4" feederNumber="17" laneNumber="1">
				<BadmarkReference refDes="B3" circuitNumber="4"/>
			</Pick>
			<Align moduleId="NCLA"/>
			<Place/>
			<Pick boardNumber="9" alignWith="AM_86" refDes="U4" circuitNumber="2" feederNumber="21" laneNumber="1">
				<BadmarkReference refDes="B1" circuitNumber="2"/>
			</Pick>
			<Align moduleId="CV_LFOV"/>
			<Place/>
		</Index>
	</Actions>
</PlacementProgram>
