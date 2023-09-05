def read_requirements_txt(filename):
    """
    Read a requirements.txt file and return a list of packages.
    """
    with open(filename, 'r') as f:
        lines = f.readlines()
    
    # Filter out comments and blank lines
    packages = [line.strip() for line in lines if line.strip() and not line.startswith('#')]
    
    return packages

def write_environment_yaml(filename, packages):
    """
    Write an environment.yaml file given a list of packages.
    """
    with open(filename, 'w') as f:
        f.write("name: my_environment\n")
        f.write("channels:\n")
        f.write("  - defaults\n")
        f.write("dependencies:\n")
        
        # Conda packages
        f.write("  - python=3.8\n")
        
        # Pip packages
        f.write("  - pip\n")
        f.write("  - pip:\n")
        
        for package in packages:
            f.write(f"    - {package}\n")

if __name__ == '__main__':
    requirements_file = 'requirements.txt'
    environment_file = 'environment.yaml'
    
    # Read requirements.txt
    packages = read_requirements_txt(requirements_file)
    
    # Write environment.yaml
    write_environment_yaml(environment_file, packages)
