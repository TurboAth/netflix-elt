document.addEventListener('DOMContentLoaded', function() {
    // Find all tab containers on the page
    const tabContainers = document.querySelectorAll('.tab-container');

    tabContainers.forEach(container => {
        // Get buttons and content panes for each container
        const buttons = container.querySelectorAll('.tab-button');
        const contents = container.querySelectorAll('.tab-content');

        buttons.forEach(button => {
            button.addEventListener('click', () => {
                const tabName = button.getAttribute('data-tab');

                // Deactivate all buttons and content in this group
                buttons.forEach(btn => btn.classList.remove('active'));
                contents.forEach(content => content.classList.remove('active'));

                // Activate the clicked button and its corresponding content
                button.classList.add('active');
                container.querySelector(`.tab-content[data-tab="${tabName}"]`).classList.add('active');
            });
        });
    });
});
