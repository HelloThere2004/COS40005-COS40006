import React from 'react';

interface HeaderProps {
  activeItem: string;
  onNavItemClick: (item: string) => void;
}

const Header: React.FC<HeaderProps> = ({ activeItem, onNavItemClick }) => {
  const navItems = ['Home', 'Dashboard', 'Detailed', 'Validated Volum'];

  return (
    <header className="bg-[#232F3E] text-white shadow-md">
      <div className="container mx-auto px-4 sm:px-6 lg:p-8">
        <div className="flex h-16 items-center justify-between">
          <div className="flex items-center">
            <span className="text-2xl font-bold tracking-wider">FCAJ</span>
          </div>
          <nav className="hidden md:flex items-center space-x-4">
            {navItems.map((item) => (
              <a
                key={item}
                href="#"
                onClick={(e) => {
                  e.preventDefault();
                  onNavItemClick(item);
                }}
                className={`px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                  activeItem === item
                    ? 'bg-gray-700 text-white'
                    : 'text-gray-300 hover:bg-gray-700 hover:text-white'
                }`}
              >
                {item}
              </a>
            ))}
          </nav>
        </div>
      </div>
    </header>
  );
};

export default Header;