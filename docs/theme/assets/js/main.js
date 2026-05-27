// Zeitung Theme - Main JavaScript

// Add event listener when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {

  // Theme toggle functionality
  const themeToggle = document.getElementById('theme-toggle');
  const themeIcon = document.querySelector('.theme-icon');

  // Get stored theme or default to dark
  const getStoredTheme = () => localStorage.getItem('dev-theme') || 'dark';
  const setStoredTheme = (theme) => localStorage.setItem('dev-theme', theme);

  // Apply theme (theme is already set inline in head, but we need to update icon)
  const setTheme = (theme) => {
    document.documentElement.setAttribute('data-theme', theme);
    if (themeIcon) {
      themeIcon.textContent = theme === 'light' ? '🌙' : '☀️';
    }
  };

  // Initialize theme icon (theme attribute already set inline)
  const currentTheme = document.documentElement.getAttribute('data-theme') || getStoredTheme();
  setTheme(currentTheme);

  // Toggle theme on click
  themeToggle?.addEventListener('click', () => {
    const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
    setStoredTheme(newTheme);
  });

  // Smooth scrolling for anchor links with URL hash update
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', (e) => {
      const hash = anchor.getAttribute('href');
      const target = document.querySelector(hash);
      if (target) {
        e.preventDefault();
        target.scrollIntoView({ behavior: 'smooth' });
        // Update URL hash so the link is shareable
        history.pushState(null, '', hash);
      }
    });
  });

  // Scroll to anchor on initial page load if URL contains a hash
  if (window.location.hash) {
    const target = document.querySelector(window.location.hash);
    if (target) {
      // Small delay to ensure layout is settled
      requestAnimationFrame(() => {
        target.scrollIntoView({ behavior: 'smooth' });
      });
    }
  }

  // Highlight active page in navigation
  const currentPath = window.location.pathname;
  document.querySelectorAll('.td-sidebar-link').forEach(link => {
    if (link.getAttribute('href') === currentPath) {
      link.classList.add('active');
    }
  });

  // TOC active section highlighting
  const tocLinks = document.querySelectorAll('.td-toc nav a');
  const headings = document.querySelectorAll('main h1, main h2, main h3, main h4, main h5, main h6');

  if (tocLinks.length && headings.length) {
    const highlightTocLink = () => {
      let current = '';
      const scrollPos = window.scrollY + 100; // Offset for navbar

      headings.forEach(heading => {
        const top = heading.offsetTop;
        if (scrollPos >= top) {
          current = heading.id;
        }
      });

      tocLinks.forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('href') === `#${current}`) {
          link.classList.add('active');
        }
      });
    };
    window.addEventListener('scroll', highlightTocLink);
    highlightTocLink();
  }
});
