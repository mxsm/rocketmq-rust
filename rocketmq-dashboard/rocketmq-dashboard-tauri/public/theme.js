// Runs before styles or React so saved preferences also apply to the first paint.
(function () {
  var theme = 'dark';
  try {
    if (localStorage.getItem('rocketmq-dashboard-theme') === 'light') theme = 'light';
  } catch (_) {
    // A restricted WebView can still use the default theme without persistence.
  }
  var root = document.documentElement;
  root.dataset.theme = theme;
  root.classList.toggle('dark', theme === 'dark');
  root.style.colorScheme = theme;
  root.style.backgroundColor = theme === 'dark' ? '#171719' : '#f5f5f7';
})();
