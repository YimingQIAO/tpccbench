# Set the project name
project(raman)

# Glob all source files, named SRC_FILES
file(GLOB_RECURSE SRC_FILES "*.cpp")

# Create a sources variable with a link to all cpp files to compile
add_library(${PROJECT_NAME} ${SRC_FILES})