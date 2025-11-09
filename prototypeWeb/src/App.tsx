import Dashboard from './components/Dashboard'
import './App.css'
import DetailedPage from './components/DetailedPage';
import React from 'react';
import Header from './components/Header';
import ValidatedVolumPage from './components/ValidatedVolumPage';
import FileUploader from './components/FileUploader';

function App() {
  const [currentPage, setCurrentPage] = React.useState('Home');
  const renderPage = () => {
    switch (currentPage) {
      case 'Detailed':
        return <DetailedPage />;
      case 'Dashboard':
        return <Dashboard />;
      case 'Validated Volum':
        return <ValidatedVolumPage />;
      default:
        return <FileUploader />;
    }
  };
  return (
    <>
      <div className="min-h-screen bg-gray-100 text-gray-800">
        <Header activeItem={currentPage} onNavItemClick={setCurrentPage} />
          {renderPage()}
      </div>
    </>
  )
}

export default App
