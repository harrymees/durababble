document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.alert-foldable .alert-heading').forEach(heading => {
    heading.addEventListener('click', () => {
      heading.closest('.alert-foldable').classList.toggle('alert-collapsed');
    });
  });
});
