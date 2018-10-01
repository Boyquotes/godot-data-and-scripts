# godot-scripts
This repository holds a collection of scripts used in the GODOT project, mainly for extracting, transforming and loading data into the GODOT graph.

**create_godot_graph.py**

This script creates the nucleus and scaffold of the GODOT graph: Starting from the Timline node, which is the root of the complete GODOT graph, it builds up paths for some basic ancient dating systems like Roman consulships, reigns of Egyptian Ptolemies etc. It adds basic properties for CalendarPart nodes and GODOT nodes.
 
**create_roman_imperial_titulature.py**

This script creates the scaffold of the following parts of the Roman imperial titulature:
- tribunicia potestas
- imperial acclamations
- imperial consulships
- victory titles
 