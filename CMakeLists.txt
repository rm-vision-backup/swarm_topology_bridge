cmake_minimum_required(VERSION 3.0.2)
project(swarm_topology_bridge)

find_package(catkin REQUIRED COMPONENTS
  geographic_msgs
  rospy
  roslib
  std_msgs
  geometry_msgs
  sensor_msgs
  nav_msgs
  mavros_msgs
  swarm_uav_interfaces
)

catkin_package(
  CATKIN_DEPENDS geographic_msgs rospy roslib std_msgs geometry_msgs sensor_msgs nav_msgs mavros_msgs swarm_uav_interfaces
)

if(CATKIN_ENABLE_TESTING)
  catkin_add_nosetests(test/test_intent_serialization.py)
  catkin_add_nosetests(test/test_geographic_dependency.py)
  catkin_add_nosetests(test/test_bridge_health.py)
  catkin_add_nosetests(test/test_bundle.py)
endif()

install(PROGRAMS
  scripts/bridge_node.py
  scripts/test_chatter.py
  scripts/test_service_node.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

# 数据面观测/链路加固模块：与 bridge_node.py 同目录，devel/install 空间必须有它，
# 否则 relay 起来后 `import bridge_health` 失败（桥接节点直接起不来）。
install(FILES
  scripts/bridge_health.py
  scripts/bundle.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

install(DIRECTORY config
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)
