import React from 'react';
import { Article, ArticleStatus } from '../types';
import ArrowLeftIcon from './icons/ArrowLeftIcon';
import FilterIcon from './icons/FilterIcon';
import ChartsPanel from './ChartsPanel';

// Mock data for the dashboard
const initialArticles: Article[] = [
  {
    id: 'ART-001',
    name: 'Giới thiệu về React Hooks',
    status: ArticleStatus.PUBLISHED,
  },
  {
    id: 'ART-002',
    name: 'Hướng dẫn Tailwind CSS cho người mới bắt đầu',
    status: ArticleStatus.PUBLISHED,
  },
  {
    id: 'ART-003',
    name: 'Tối ưu hóa hiệu suất ứng dụng TypeScript',
    status: ArticleStatus.DRAFT,
  },
  {
    id: 'ART-004',
    name: 'Kiến trúc Micro-frontend (Bài cũ)',
    status: ArticleStatus.ARCHIVED,
  },
  {
    id: 'ART-005',
    name: 'Xử lý trạng thái với Redux Toolkit',
    status: ArticleStatus.PUBLISHED,
  },
];

// Sub-component for the header
const Header: React.FC = () => {
  return (
    <header className="bg-[#232F3E] text-white shadow-md">
      <div className="container mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex h-16 items-center justify-between">
          <button className="flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium text-gray-300 transition-colors hover:bg-gray-700 hover:text-white">
            <ArrowLeftIcon className="h-5 w-5" />
            <span className="hidden sm:inline">Return</span>
          </button>
          <h1 className="text-lg font-semibold sm:text-xl">Article Management</h1>
          <button className="rounded-md bg-gray-700 p-2 text-gray-300 transition-colors hover:bg-gray-600 hover:text-white">
            <FilterIcon className="h-5 w-5" />
          </button>
        </div>
      </div>
    </header>
  );
};

// Sub-component for a single table row
interface ArticleRowProps {
  article: Article;
}

const ArticleRow: React.FC<ArticleRowProps> = ({ article }) => {
  return (
    <tr className="border-b border-gray-200 transition-colors hover:bg-gray-50">
      <td className="whitespace-nowrap px-6 py-4 text-sm font-medium text-gray-500">{article.id}</td>
      <td className="whitespace-nowrap px-6 py-4 text-sm font-semibold text-gray-900">{article.name}</td>
      <td className="px-6 py-4 text-sm">
        <a
          href="#"
          className="rounded-full bg-blue-100 px-3 py-1 text-xs font-semibold text-blue-800 transition-colors hover:bg-blue-200"
        >
          View Details
        </a>
      </td>
    </tr>
  );
};

// Main Dashboard Component
const Dashboard: React.FC = () => {
  const [articles] = React.useState<Article[]>(initialArticles);

  return (
    <div className="flex h-full flex-col">
      <Header />
      <main className="flex-grow bg-gray-50 p-4 sm:p-6 lg:p-8">
        <div className="container mx-auto">
          <div className="grid grid-cols-1 gap-8 lg:grid-cols-12">
            <div className="lg:col-span-8 xl:col-span-9">
              <div className="overflow-hidden rounded-lg bg-white shadow-lg">
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th
                          scope="col"
                          className="px-6 py-3 text-left text-xs font-bold uppercase tracking-wider text-gray-500"
                        >
                          ID
                        </th>
                        <th
                          scope="col"
                          className="px-6 py-3 text-left text-xs font-bold uppercase tracking-wider text-gray-500"
                        >
                          Tên bài
                        </th>
                        <th
                          scope="col"
                          className="px-6 py-3 text-left text-xs font-bold uppercase tracking-wider text-gray-500"
                        >
                          Detail
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200 bg-white">
                      {articles.map((article) => (
                        <ArticleRow key={article.id} article={article} />
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
            <div className="lg:col-span-4 xl:col-span-3">
              <ChartsPanel articles={articles} />
            </div>
          </div>
        </div>
      </main>
    </div>
  );
};

export default Dashboard;