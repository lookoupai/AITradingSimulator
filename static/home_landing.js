class HomeLandingPage {
    async init() {
        try {
            const response = await fetch('/api/auth/me', { credentials: 'include' });
            if (!response.ok) {
                return;
            }
            const loginButton = document.getElementById('loginBtn');
            const dashboardButton = document.getElementById('dashboardBtn');
            if (loginButton) {
                loginButton.style.display = 'none';
            }
            if (dashboardButton) {
                dashboardButton.style.display = 'inline-flex';
            }
        } catch (error) {
            console.error('Failed to check login status:', error);
        }
    }
}

new HomeLandingPage().init();
